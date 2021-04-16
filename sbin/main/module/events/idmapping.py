# encoding:utf-8
__author__ = 'zhangjt'
import json
import logging
import time
import uuid

from module.const import argparse
from module.const.common import mock_flag
from module.const.main_class import *
from module.rpc import AbstractRpcHandler
from module.tools import cfg_parser, hbase, shell, hadoop, hdfs, utils
from module.tools.env import dataengine_env
from module.tools.hive import Hive
from module.tools.spark import Spark2
from module.tools.cfg import prop_utils, app_utils
from module.const.version import __version__ as version


class ToolSparkAdapter(AbstractRpcHandler):
    def __init__(self, parent):
        AbstractRpcHandler.__init__(self)
        self.spark2 = Spark2(rpcHandler=self)
        self.application_id = None
        self.job_id = str(uuid.uuid4())
        self.parent = parent

    def submit(self, args, clazz, driver_memory="4G", executor_memory="12G", conf=None, jars=None,
               props=None, hdfs_jars=None):
        default_conf = {
            "spark.driver.cores": "4",
            "spark.sql.shuffle.partitions": "1024",
            "spark.speculation.quantile": "0.99",
            "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors,
        }
        default_props = {
            "--class": clazz,
            "--driver-memory": driver_memory,
            "--executor-memory": executor_memory,
            "--executor-cores": "4"
        }

        if conf is not None:
            default_conf.update(conf)
        if props is not None:
            default_props.update(props)
        return self.spark2.submit(
            module="dataengine-utils",
            args=args + " --rpcHost " + self.spark2.rpc_server.host + " --rpcPort " + str(self.spark2.rpc_server.port),
            job_name=self.parent.job_name,
            job_id=self.job_id,
            props=default_props,
            conf=default_conf,
            jars=jars,
            hdfs_jars=hdfs_jars
        )

    def receive(self, param):
        arr = str.split(str(param), u'\u0001')
        event_code = str(arr[0])
        if event_code == "1":
            # {code}\u0001{application id}
            self.application_id = str(arr[1])
        else:
            logging.error("rpc handler code[%s] error." % event_code)
            return False
        return True


class Tools:
    def __init__(self):
        self.day = utils.current_day()
        self.update_time = utils.yesterday()
        self.job_name = "id_mapping_tool-%s" % self.day

    # device,mac,imei,imei_14,idfa,phone,imsi
    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--gen_hive_data', action='store_true', default=False,
                            help='生成hive表, src=dm_sdk_master.android_id_full,dm_sdk_master.ios_id_full')
        namespace, other_args = parser.parse_known_args(args=args)

        if namespace.gen_hive_data:
            self.gen_hive_data(other_args)
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()

    def _add_id_types(self, parser):
        parser.add_argument('--device', action='store_true', default=False)
        parser.add_argument('--mac', action='store_true', default=False)
        parser.add_argument('--imei', action='store_true', default=False)
        parser.add_argument('--imei_14', action='store_true', default=False)
        parser.add_argument('--phone', action='store_true', default=False)
        parser.add_argument('--idfa', action='store_true', default=False)
        parser.add_argument('--imsi', action='store_true', default=False)
        parser.add_argument('--serialno', action='store_true', default=False)
        parser.add_argument('--external_id', action='store_true', default=False)
        parser.add_argument('--oaid', action='store_true', default=False)

    def _get_other_id_types(self, namespace):
        arr = []
        if namespace.mac:
            arr.append("mac")
        if namespace.imei:
            arr.append("imei")
        if namespace.imei_14:
            arr.append("imei_14")
        if namespace.phone:
            arr.append("phone")
        if namespace.idfa:
            arr.append("idfa")
        if namespace.imsi:
            arr.append("imsi")
        if namespace.serialno:
            arr.append("serialno")
        if namespace.oaid:
            arr.append("oaid")

        return arr

    def _get_all_id_types(self, namespace):
        arr = []
        if namespace.device:
            arr.append("device")
        if namespace.mac:
            arr.append("mac")
        if namespace.imei:
            arr.append("imei")
        if namespace.imei_14:
            arr.append("imei_14")
        if namespace.phone:
            arr.append("phone")
        if namespace.idfa:
            arr.append("idfa")
        if namespace.imsi:
            arr.append("imsi")
        if namespace.serialno:
            arr.append("serialno")
        if namespace.external_id:
            arr.append("external_id")
        if namespace.oaid:
            arr.append("oaid")

        return arr

    # 生成hive业务表
    def gen_hive_data(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        self._add_id_types(parser)
        parser.add_argument('--day', action='store', default=self.day,
                            help='默认当前时间')
        namespace, argv = parser.parse_known_args(args=args)
        if argv:
            logging.error('unrecognized arguments: %s' % ' '.join(argv))
            parser.print_help()
            exit(-1)

        id_type_arr = self._get_all_id_types(namespace)

        hive_spark = ToolSparkAdapter(self)
        hive_spark.submit(
            " -i {id_types} -d {day} -a hive".format(day=namespace.day, id_types=",".join(id_type_arr)),
            clazz=ID_MAPPING_TOOLS,
            driver_memory="8G"
        )


class ToolsV2(Tools):
    def __init__(self):
        Tools.__init__(self)
        self.day = utils.current_day()
        self.update_time = utils.yesterday()
        self.job_name = "id_mapping_tool_v2-%s" % self.day

    # 生成hive业务表
    def gen_hive_data(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        self._add_id_types(parser)
        parser.add_argument('--day', action='store', default=self.day,
                            help='默认当前时间')
        parser.add_argument('--once', action='store_true', default=False, help='是否一次性补全量')
        parser.add_argument('--partition', action='store', default="4096", help='shuffle的分区数')
        parser.add_argument('--partition_bytes', action='store', default="33554432", help='partition容纳的最大数据量')
        parser.add_argument('--overhead', action='store', default="5120", help='memoryOverhead')
        namespace, argv = parser.parse_known_args(args=args)
        if argv:
            logging.error('unrecognized arguments: %s' % ' '.join(argv))
            parser.print_help()
            exit(-1)

        id_type_arr = self._get_all_id_types(namespace)

        hive_spark = ToolSparkAdapter(self)
        hive_spark.submit(
            " -i {id_types} -d {day} -a hive -o {once}".format(day=namespace.day, id_types=",".join(id_type_arr),
                                                               once=namespace.once),
            clazz=ID_MAPPING_TOOLS_V2,
            driver_memory="8G",
            conf={
                "spark.driver.cores": "2",
                "spark.sql.shuffle.partitions": namespace.partition,
                "spark.sql.files.maxPartitionBytes": namespace.partition_bytes,
                "spark.executor.memoryOverhead": namespace.overhead,
                "spark.driver.maxResultSize": "3g"
            },
            props={
                "--executor-memory": "9g",
                "--executor-cores": "3"
            }
        )


class ToolsSec:
    def __init__(self):
        self.day = utils.current_day()
        self.update_time = utils.yesterday()
        self.job_name = "id_mapping_sec-tool-%s" % self.day

    # device,mcid,ieid,ieid_14,ifid,pid,isid
    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--gen_hive_data', action='store_true', default=False,
                            help='生成hive表, src=dm_sdk_master.android_id_full,dm_sdk_master.ios_id_full')
        namespace, other_args = parser.parse_known_args(args=args)

        if namespace.gen_hive_data:
            self.gen_hive_data(other_args)
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()

    def _add_id_types(self, parser):
        parser.add_argument('--device', action='store_true', default=False)
        parser.add_argument('--mcid', action='store_true', default=False)
        parser.add_argument('--ieid', action='store_true', default=False)
        parser.add_argument('--ieid_14', action='store_true', default=False)
        parser.add_argument('--pid', action='store_true', default=False)
        parser.add_argument('--ifid', action='store_true', default=False)
        parser.add_argument('--isid', action='store_true', default=False)
        parser.add_argument('--snid', action='store_true', default=False)
        parser.add_argument('--external_id', action='store_true', default=False)
        parser.add_argument('--oiid', action='store_true', default=False)

    def _get_other_id_types(self, namespace):
        arr = []
        if namespace.mcid:
            arr.append("mcid")
        if namespace.ieid:
            arr.append("ieid")
        if namespace.ieid_14:
            arr.append("ieid_14")
        if namespace.pid:
            arr.append("pid")
        if namespace.ifid:
            arr.append("ifid")
        if namespace.isid:
            arr.append("isid")
        if namespace.snid:
            arr.append("snid")
        if namespace.oiid:
            arr.append("oiid")

        return arr

    def _get_all_id_types(self, namespace):
        arr = []
        if namespace.device:
            arr.append("device")
        if namespace.mcid:
            arr.append("mcid")
        if namespace.ieid:
            arr.append("ieid")
        if namespace.ieid_14:
            arr.append("ieid_14")
        if namespace.pid:
            arr.append("pid")
        if namespace.ifid:
            arr.append("ifid")
        if namespace.isid:
            arr.append("isid")
        if namespace.snid:
            arr.append("snid")
        if namespace.external_id:
            arr.append("external_id")
        if namespace.oiid:
            arr.append("oiid")

        return arr

    # 生成hive业务表
    def gen_hive_data(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        self._add_id_types(parser)
        parser.add_argument('--day', action='store', default=self.day,
                            help='默认当前时间')
        parser.add_argument('--once', action='store_true', default=False, help='是否一次性补全量')
        parser.add_argument('--partition', action='store', default="4096", help='shuffle的分区数')
        parser.add_argument('--partition_bytes', action='store', default="33554432", help='partition容纳的最大数据量')
        parser.add_argument('--overhead', action='store', default="5120", help='memoryOverhead')
        namespace, argv = parser.parse_known_args(args=args)
        if argv:
            logging.error('unrecognized arguments: %s' % ' '.join(argv))
            parser.print_help()
            exit(-1)

        id_type_arr = self._get_all_id_types(namespace)

        hive_spark = ToolSparkAdapter(self)
        hive_spark.submit(
            " -i {id_types} -d {day} -a hive -o {once}".format(day=namespace.day, id_types=",".join(id_type_arr),
                                                               once=namespace.once),
            clazz=ID_MAPPING_SEC_TOOLS,
            driver_memory="8G",
            conf={
                "spark.driver.cores": "2",
                "spark.sql.shuffle.partitions": namespace.partition,
                "spark.sql.files.maxPartitionBytes": namespace.partition_bytes,
                "spark.executor.memoryOverhead": namespace.overhead,
                "spark.driver.maxResultSize": "3g"
            },
            props={
                "--executor-memory": "9g",
                "--executor-cores": "3"
            },
            hdfs_jars={
                'hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar',
                'hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar'
            }
        )


class SnsuidMapping(AbstractRpcHandler):
    def __init__(self, args):
        AbstractRpcHandler.__init__(self)
        self.spark2 = Spark2(rpcHandler=self)
        if len(args) is 0:
            args = ["-h"]
        from module.const.argparse import ArgumentParser
        parser = ArgumentParser()
        parser.add_argument('-d', '--day', action='store', default=False, help='数据日期')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info(namespace)
        logging.info(other_args)

        self.day=namespace.day
        self.job_name = "SnsuidMapping[%s]" % self.day
        self.job_id = str(uuid.uuid4())

    def submit(self):
        self.spark2.submit(
            "dataengine-utils", self.day, self.job_name + '_' + self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.idmapping.SnsuidDeviceMapping",
                "--driver-memory": "4g",
                "--executor-memory": "12g",
                "--executor-cores": "4"
            },
            conf={
                "spark.dynamicAllocation.maxExecutors": "60",
                "spark.sql.shuffle.partitions": "5000"
            }
        )
        return 0
