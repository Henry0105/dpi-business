# coding=utf-8
# 标签
from module.const.common import mock_flag
from module.tools.cfg import prop_utils
from module.tools.env import dataengine_env

__author__ = 'zhangjt'
import json
import logging
import time
import uuid

from module.const import argparse
from module.rpc import AbstractRpcHandler
from module.tools import *
from module.tools.hive import Hive
from module.tools.hbase import DataIncrExport
from module.tools.hfile import LoadHfile
from module.tools.http import HttpClient
from module.tools.spark import Spark2
from module.tools.mysql import MySQLClient
from module.tools.cfg import app_utils


# http://c.mob.com/pages/viewpage.action?pageId=6654628
class DeviceTags:

    def __init__(self):
        self.day = utils.current_day()

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--gen_hive_data', action='store_true', default=False, help='生成hive分区')
        parser.add_argument('--sample', action='store_true', default=False, help='gen_hive_data部分数据')
        parser.add_argument('--export_hbase_full', action='store_true', default=False, help='全量导入hbase,通过hfile')
        parser.add_argument('--export_hbase_incr', action='store_true', default=False, help='增量导入hbase,通过api')
        parser.add_argument('--day', action='store', default=self.day, help='默认当前时间')
        parser.add_argument('--tags_table_name', action='store', default="", help='增量导入hbase table')
        namespace, other_args = parser.parse_known_args(args=args)

        if namespace.gen_hive_data:
            TagsHiveGenerator(namespace.day, namespace.sample).gen_hive_data()
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()


class TagsChecker(AbstractRpcHandler):
    def __init__(self):
        AbstractRpcHandler.__init__(self)
        self.spark2 = Spark2(rpcHandler=self)
        self.job_name = "TagsChecker[%s]" % utils.current_day()
        self.job_id = str(uuid.uuid4())
        self.application_id = None
        self.devices = []

    def receive(self, param):
        arr = str.split(str(param), u'\u0001')
        event_code = str(arr[0])
        if event_code == "1":
            # {code}\u0001{application id}
            self.application_id = str(arr[1])
        elif event_code == "2":
            # {device1}\u0002{device2}
            self.devices = str.split(str(arr[1]), u'\u0002')
        else:
            logging.error("rpc handler code[%s] error." % event_code)
            return False
        return True

    def submit(self, hbase_table_name):
        labels = cfg_parser.get("hbase_cluster").split(",")
        zk_group = ";".join(
            map(lambda label: cfg_parser.get("hbase_zk_%s" % label), labels)
        )

        self.spark2.submit(
            "dataengine-utils", " ".join([
                "--tableName %s" % hbase_table_name,
                "--zk '%s'" % zk_group,
                "--rpcHost %s" % self.spark2.rpc_server.host,
                "--rpcPort %s" % self.spark2.rpc_server.port,
            ]), self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.tags.TagsChecker",
                "--driver-memory": "4g",
                "--executor-memory": "10g",
                "--executor-cores": "4",
            },
            conf={
                "spark.dynamicAllocation.maxExecutors": "60",
            })

        logging.info(self.devices)
        return self.devices


class TagsHiveGenerator:

    def __init__(self, day=None, sample=False):
        self.hive_table_name = prop_utils.HIVE_TABLE_DEVICE_PROFILE_INFO_SEC
        if day is not None:
            self.day = day
        else:
            self.day = utils.current_day()
        self.job_name = "TagsHFileGenerator[%s]" % self.day
        self.job_id = str(uuid.uuid4())
        self.sample = sample

    def gen_hive_data(self):
        args = " ".join([
            "--updateTime %s" % self.day,
            "--hiveTableName %s" % self.hive_table_name,
            "--sample %s" % self.sample
        ])
        spark2 = Spark2()
        watch = Stopwatch()
        logging.info("create hive par is beginning %s ..." % watch.format_current_time())
        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.tags.TagsHiveGenerator",
                "--driver-memory": "6g",
                "--executor-memory": "20g",
                "--executor-cores": "4",
            },
            conf={
                "spark.sql.shuffle.partitions": "4096",
                "spark.executor.memoryOverhead": "5120",
                "spark.speculation.quantile": "0.99",
                "spark.dynamicAllocation.maxExecutors": "100",
            },
            files={
                "application.properties"
            }
        )

        logging.info("create hive par is ended %s ." % watch.format_current_time())
        logging.info("create hive par cost %s .\n\n\n" % watch.watch())

        return 0


class IosDeviceTags:

    def __init__(self):
        self.day = utils.current_day()

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--gen_hive_data', action='store_true', default=False, help='生成hive分区')
        parser.add_argument('--sample', action='store_true', default=False, help='gen_hive_data部分数据')
        parser.add_argument('--export_hbase_full', action='store_true', default=False, help='全量导入hbase,通过hfile')
        parser.add_argument('--export_hbase_incr', action='store_true', default=False, help='增量导入hbase,通过api')
        parser.add_argument('--day', action='store', default=self.day, help='默认当前时间')
        parser.add_argument('--tags_table_name', action='store', default="", help='增量导入hbase table')
        namespace, other_args = parser.parse_known_args(args=args)

        if namespace.gen_hive_data:
            IosTagsHiveGenerator(namespace.day, namespace.sample).gen_hive_data()
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()


class IosTagsHiveGenerator:
    def __init__(self, day=None, sample=False):
        self.hive_table_name = prop_utils.HIVE_TABLE_IDFA_PROFILE_INFO
        if day is not None:
            self.day = day
        else:
            self.day = utils.current_day()
        self.job_name = "IosTagsHiveGenerator[%s]" % self.day
        self.job_id = str(uuid.uuid4())
        self.sample = sample

    def gen_hive_data(self):
        args = " ".join([
            "--updateTime %s" % self.day,
            "--hiveTableName %s" % self.hive_table_name,
            "--sample %s" % self.sample
        ])
        spark2 = Spark2()
        watch = Stopwatch()
        logging.info("create hive par is beginning %s ..." % watch.format_current_time())
        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.tags.IosTagsHiveGenerator",
                "--driver-memory": "4g",
                "--executor-memory": "10g",
                "--executor-cores": "4",
            },
            conf={
                "spark.sql.shuffle.partitions": "64",
                "spark.executor.memoryOverhead": "5120",
                "spark.speculation.quantile": "0.99",
                "spark.dynamicAllocation.maxExecutors": "100",
            })

        logging.info("create hive par is ended %s ." % watch.format_current_time())
        logging.info("create hive par cost %s .\n\n\n" % watch.watch())

        return 0


class IdTagsLatest:
    def __init__(self):
        self.day = utils.current_day()

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--gen_hive_data', action='store_true', default=False, help='生成hive分区')
        parser.add_argument('--gen_sec_hive_data', action='store_true', default=False, help='生成hive分区')
        parser.add_argument('--sample', action='store_true', default=False, help='gen_hive_data部分数据')
        parser.add_argument('--day', action='store', default=self.day, help='默认当前时间')
        parser.add_argument('--id_types', action='store', default='device,imei,mac,phone,enhance', help='device,imei,mac,phone,enhance')
        parser.add_argument('--partitions', action='store', default='5000', help='shuffle的分区数')
        parser.add_argument('--executor_memory', action='store', default='12g', help='executor内存数')
        namespace, other_args = parser.parse_known_args(args=args)

        if namespace.gen_hive_data:
            IdTagsLatestHiveGenerator(namespace.day, namespace.sample, namespace.id_types,
                                      namespace.partitions, namespace.executor_memory).gen_hive_data()
        elif namespace.gen_sec_hive_data:
            IdTagsSecLatestHiveGenerator(namespace.day, namespace.sample, namespace.id_types,
                                      namespace.partitions, namespace.executor_memory).gen_hive_data()
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()


class TagsTool:
    def __init__(self):
        self.day = utils.current_day()

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--gen_hive_data', action='store_true', default=False, help='生成hive分区')
        parser.add_argument('--sample', action='store', default=False, help='gen_hive_data部分数据')
        parser.add_argument('--day', action='store', default=self.day, help='默认当前时间')
        parser.add_argument('--batch', action='store', default=10, help='多少个分区做一次checkpoint')
        parser.add_argument('--profile_ids', action='store', help='带有版本号的标签id,用逗号分割')
        parser.add_argument('--hbase_profile_ids', action='store', default="", help='需要导入hbase的带有版本号的标签id,用逗号分割')
        parser.add_argument('--export_hbase', action='store_true', help='调用导hbase工具导数据')
        parser.add_argument('--hbase_table', action='store', help='hbase表')
        parser.add_argument('--hbase_cluster', action='store', help='hbase集群')
        parser.add_argument('--keep_src_hfile', action='store_true', default=False, help='是否保留生成的hfile')
        namespace, other_args = parser.parse_known_args(args=args)

        if namespace.gen_hive_data:
            TagsToolHiveGenerator(namespace.day, namespace.profile_ids, namespace.hbase_profile_ids)\
                .gen_hive_data(namespace.sample, namespace.batch)
        elif namespace.export_hbase:
            TagsToolHiveGenerator(namespace.day, namespace.profile_ids)\
                .export_hbase(namespace.hbase_table, namespace.hbase_cluster, namespace.keep_src_hfile)
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()

class FullTagsGenerator:
    def __init__(self, day=None):
        if day is not None:
            self.day = day
        else:
            self.day = utils.current_day()

        self.job_name = "FullTagsGenerator[%s]" % self.day
        self.job_id = str(uuid.uuid4())

    """
    初始化全量标签
    :usage: ./dataengine-tools.sh --tags_integration_tool  --init_full_tags --day 20190201
    """
    def init_full_tags(self, sample=False, batch=10):
        self.sample = sample
        self.batch = batch
        args = " ".join([
            "--day %s" % self.day,
            "--sample %s" % self.sample,
            "--batch %s" % self.batch
        ])
        spark2 = Spark2()
        watch = Stopwatch()

        logging.info("init_full_tags is beginning %s ..." % watch.format_current_time())
        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.tags.fulltags.FullTagsGenerator",
                "--driver-memory": "9g",
                "--executor-memory": "12g",
                "--executor-cores": "3"
            },
            conf={
                "spark.sql.shuffle.partitions": "30000",
                "spark.speculation.quantile": "0.99",
                "spark.speculation.multiplier": "3",
                "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors,
                "spark.driver.maxResultSize": "8g",
                "spark.executor.memoryOverhead": "5120",
                "spark.network.timeout": "1000000",
                "spark.executor.heartbeatInterval": "60s"
            },
            files=['application.properties']
        )

        logging.info("init_full_tags is ended %s ." % watch.format_current_time())
        logging.info("init_full_tags cost %s .\n\n\n" % watch.watch())

        return 0

    """
    更新全量标签表
    :usage: ./dataengine-tools.sh --tags_integration_tool  --update_full_tags --day 20190201
    """
    def update_full_tags(self, partitions, memory_overhead):
        args = " ".join([
            "--day %s" % self.day
        ])
        spark2 = Spark2()
        watch = Stopwatch()

        logging.info("update_full_tags is beginning %s ..." % watch.format_current_time())
        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.tags.fulltags.UpdateFullTags",
                "--driver-memory": "9g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
            },
            conf={
                "spark.sql.shuffle.partitions": partitions,
                "spark.speculation.quantile": "0.99",
                "spark.speculation.multiplier": "3",
                "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors,
                "spark.executor.memoryOverhead": memory_overhead,
                "spark.network.timeout": "1000000",
                "spark.executor.heartbeatInterval": "60s"
            },
            files=['application.properties']
        )

        logging.info("update_full_tags is ended %s ." % watch.format_current_time())
        logging.info("update_full_tags cost %s .\n\n\n" % watch.watch())

        return 0


class TagsToolHiveGenerator:
    def __init__(self, day=None, profile_ids=None, hbase_profile_ids=None):
        if day is not None:
            self.day = day
        else:
            self.day = utils.current_day()

        if profile_ids is not None:
            self.profile_ids = profile_ids
        else:
            query = """
                select concat(a.profile_id, '_', a.profile_version_id) as full_id
                from t_individual_profile as a
                inner join t_profile_version as b
                on a.profile_id = b.profile_id and a.profile_version_id = b.profile_version_id
                inner join t_profile_metadata as d
                on a.profile_id = d.profile_id
                inner join t_profile_category as c
                on d.profile_category_id = c.profile_category_id
                where a.profile_datatype in ('int', 'string', 'boolean', 'double', 'bigint')
                  AND (a.profile_table not like '%idfa%' AND a.profile_table not like '%ios%'
                    AND a.profile_table not like '%imei%' AND a.profile_table not like '%mac%'
                    AND a.profile_table not like '%phone%' AND a.profile_table not like '%serialno%'
                    and a.profile_table not in('rp_device_finance_risk_assessment_mf_v1',
                    'ronghui_product2_v1', 'ronghui_product7_v1', 'mobfin_pid_profile_lable_full_par',
                     'jiexin_xgb_classification', 'ifid_ip_location_sec_df'))
                  AND b.is_avalable = 1 AND b.is_visible = 1
            """
            rows = TagsToolHiveGenerator.query_rows(query)
            self.profile_ids = ",".join([row['full_id'] for row in rows])

        if hbase_profile_ids is not None:
            self.hbase_profile_ids = hbase_profile_ids
        else:
            query = """
                SELECT concat(c.profile_id, '_', c.profile_version_id) as full_id
                FROM (
                    SELECT id
                    FROM label_output
                    WHERE status in ({status})
                ) a
                JOIN label_output_profile b
                ON a.id = b.output_id
                JOIN t_profile_version c
                ON b.profile_version_id = c.id
                GROUP BY c.profile_id, c.profile_version_id
            """.format(status='5,6,7,8,9')
            rows = TagsToolHiveGenerator.query_rows(query)
            self.hbase_profile_ids = ",".join([row['full_id'] for row in rows])

        self.job_name = "TagsToolHiveGenerator[%s]" % self.day
        self.job_id = str(uuid.uuid4())

    def gen_hive_data(self, sample=False, batch=10):
        """
        :param sample:
        :return:
        :usage: ./dataengine-tools.sh --tags_integration_tool  --gen_hive_data --day 20190201
        """
        args = " ".join([
            "--day %s" % self.day,
            "--profile_ids %s" % self.profile_ids,
            "--hbase_profile_ids %s" % self.hbase_profile_ids,
            "--sample %s" % sample,
            "--batch %s" % batch
        ])
        spark2 = Spark2()
        watch = Stopwatch()
        logging.info("create hive par is beginning %s ..." % watch.format_current_time())
        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.tags.TagsGenerator",
                "--driver-memory": "9g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
            },
            conf={
                "spark.sql.shuffle.partitions": "7000",
                "spark.speculation.quantile": "0.99",
                "spark.speculation.multiplier": "3",
                "spark.dynamicAllocation.maxExecutors": "100",
                "spark.driver.maxResultSize": "8g",
                "spark.executor.memoryOverhead": "5120"
            },
            files=['application.properties']
        )

        logging.info("create hive par is ended %s ." % watch.format_current_time())
        logging.info("create hive par cost %s .\n\n\n" % watch.watch())

        return 0

    @staticmethod
    def load_mysql_properties():
        prop = {
            'host': app_utils.get_property("tag.mysql.ip"),
            'port': int(app_utils.get_property("tag.mysql.port")),
            'user': app_utils.get_property("tag.mysql.user"),
            'password': app_utils.get_property("tag.mysql.password"),
            'db': app_utils.get_property("tag.mysql.database")
        }
        return prop

    @staticmethod
    def query_rows(query):
        prop = TagsToolHiveGenerator.load_mysql_properties()
        client = MySQLClient(**prop)
        rows = client.query_rows(query)
        client.close()
        return rows

    def export_hbase(self, hbase_table, hbase_cluster, keep_src_hfile):
        ids = [full_id.split('_')[0] for full_id in self.hbase_profile_ids.split(',')]
        query = """ select c.profile_category_parent_id
            from (select profile_id, profile_category_id from t_profile_metadata where profile_id in (%s)) as b
            inner join t_profile_category as c
            on b.profile_category_id = c.profile_category_id
            group by c.profile_category_parent_id
        """ % ",".join(ids)

        prop = self.load_mysql_properties()
        client = MySQLClient(**prop)
        rows = client.query_rows(query)
        client.close()

        parent_ids = [row['profile_category_parent_id'] for row in rows]

        columns = ",".join(["grouped_tags['{id}'] as cateid_{id}".format(id=id) for id in parent_ids])
        confidence = """
            if(size(confidence) < 1, null,
              concat(concat_ws('\u0001', map_keys(confidence)),
                     '\u0002',
                     concat_ws('\u0001', map_values(confidence))
              )
            ) as confidence
        """

        hive_query = """
            select device, {columns}, {confidence}
            from dm_dataengine_tags.dm_tags_info
            where day='{day}' and grouped_tags is not null and size(grouped_tags) > 0 and length(device) = 40
              and device rlike '^[0-9a-f]{{40,40}}$'
        """.format(day=self.day, columns=columns, confidence=confidence)

        status, stdout = shell.submit_with_stdout("""
            /opt/mobdata/sbin/mobdata-tools2 --hbase --gen_hfile --project_name dataengine \
              --row_key device --sql "{hive_query}" --hbase_table_name {hbase_table} {keep_src_hfile} \
              --hbase_cluster {hbase_cluster} --m 13 --bandwidth 13 --executor_memory 32g
        """.format(hive_query=hive_query, hbase_table=hbase_table,
                   keep_src_hfile="--keep_src_hfile" if keep_src_hfile else "",
                   hbase_cluster=hbase_cluster))

        if status is 0:
            print("succeed")
        else:
            raise Exception("execute error %d" % status)


class IdTagsLatestHiveGenerator:
    def __init__(self, day=None, sample=False, id_types=None, partitions=None, executor_memory=None):
        if day is not None:
            self.day = day
        else:
            self.day = utils.current_day()
        self.job_name = "TagsHiveLatestGenerator[%s]" % self.day
        self.job_id = str(uuid.uuid4())
        self.sample = sample
        self.id_types = id_types
        self.partitions = partitions
        self.executor_memory = executor_memory

    def gen_hive_data(self):
        args = " ".join([
            "--day %s" % self.day,
            "--id_types %s" % self.id_types,
            "--sample %s" % self.sample
        ])
        spark2 = Spark2()
        watch = Stopwatch()
        logging.info("create hive par is beginning %s ..." % watch.format_current_time())
        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.tags.ids.TagsHiveLatestGenerator",
                "--driver-memory": "6g",
                "--executor-memory": self.executor_memory,
                "--executor-cores": "4",
            },
            conf={
                "spark.sql.shuffle.partitions": self.partitions,
                "spark.speculation.quantile": "0.85",
                "spark.driver.cores": "4",
                "spark.driver.maxResultSize": "2g",
                "spark.speculation.multiplier": "3",
                "spark.executor.userClassPathFirst": "true",
                "spark.executor.memoryOverhead": "5120"
            }
        )

        logging.info("create hive par is ended %s ." % watch.format_current_time())
        logging.info("create hive par cost %s .\n\n\n" % watch.watch())

        return 0


class IdTagsSecLatestHiveGenerator:
    def __init__(self, day=None, sample=False, id_types=None, partitions=None, executor_memory=None):
        if day is not None:
            self.day = day
        else:
            self.day = utils.current_day()
        self.job_name = "TagsSecHiveLatestGenerator[%s]" % self.day
        self.job_id = str(uuid.uuid4())
        self.sample = sample
        self.id_types = id_types
        self.partitions = partitions
        self.executor_memory = executor_memory

    def gen_hive_data(self):
        args = " ".join([
            "--day %s" % self.day,
            "--id_types %s" % self.id_types,
            "--sample %s" % self.sample
        ])
        spark2 = Spark2()
        watch = Stopwatch()
        logging.info("create hive par is beginning %s ..." % watch.format_current_time())
        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.tags.ids.TagsSecHiveLatestGenerator",
                "--driver-memory": "6g",
                "--executor-memory": self.executor_memory,
                "--executor-cores": "4",
            },
            conf={
                "spark.sql.shuffle.partitions": self.partitions,
                "spark.speculation.quantile": "0.85",
                "spark.driver.cores": "4",
                "spark.driver.maxResultSize": "2g",
                "spark.speculation.multiplier": "3",
                "spark.executor.userClassPathFirst": "true",
                "spark.executor.memoryOverhead": "5120"
            }
        )

        logging.info("create hive par is ended %s ." % watch.format_current_time())
        logging.info("create hive par cost %s .\n\n\n" % watch.watch())

        return 0


if __name__ == '__main__':
    devices = ["109e29faa91ccfe683893168f02b57e6c61e0e34", "1047b3c199a3674fa483c349e67d3d61a333a701",
               "0f88c637f65fd71cf41b355ac60ea1a02b2c75be", "0ee82a3852144614eec7315494b0759eed56f200",
               "0eb19d6edb817bee6a29ae51c9e7907415597a25", "003738e9cafda459fe5ed42e570ff354dd819c5e",
               "0c2118f8febe40391ab66f5a5ff0b9fb3f0e35c1", "0f7c76d08a696ff006f6cdfd4f71b122ab12318b",
               "10279b3910db82d948d3433e8928bd7115bcb9b2"]
