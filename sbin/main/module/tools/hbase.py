# coding=utf-8
import json
import logging
import uuid

from module.const import argparse
from module.tools import Java, utils, cfg_parser
from module.tools.env import dataengine_env
from module.tools.spark import Spark

__author__ = 'zhangjt'


class DataIncrExport:

    def __init__(self):
        pass

    def submit(self, param, job_id=str(uuid.uuid4()), num_executors="10", driver_memory="10G", executor_memory="10G",
        executor_cores="3"):
        spark = Spark()
        return spark.submit(
            args='\'%s\'' % json.dumps(param, indent=1),
            job_name="hive2hbase-incr-export",
            job_id=job_id,
            props={
                "--class": "com.mob.job.DataExport",
                "--driver-memory": driver_memory,
                "--executor-memory": executor_memory,
                "--executor-cores": executor_cores,
                "--num-executors": num_executors,
            },
            conf={
                "spark.yarn.executor.memoryOverhead": "4096",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.initialExecutors": num_executors,
                "spark.dynamicAllocation.minExecutors": num_executors,
                "spark.dynamicAllocation.maxExecutors": num_executors,

            },
            main_jar="%s/mob-data-export-2.0.3.jar" % dataengine_env.dataengine_lib_path
        )


def latest_table_name(label, table_name_regex):
    client = Java(
        jars=["conf/hadoop-conf-%s" % cfg_parser.get("cluster_current_id")] + utils.scan_files(
            dataengine_env.dataengine_home + "/lib/jars"
        )
    )

    return client.submit(
        "com.mob.dataengine.utils.hbase.LatestTable", [
            "--tableNameRegex %s" % table_name_regex,
            "--zk %s" % cfg_parser.get("hbase_zk_%s" % label)
        ]
    )


def drop_old_version_table(label, table_name_regex, versions=1):
    client = Java(
        jars=["conf/hadoop-conf-%s" % cfg_parser.get("cluster_current_id")] + utils.scan_files(
            dataengine_env.dataengine_home + "/lib/jars"
        )
    )
    client.submit(
        "com.mob.dataengine.utils.hbase.DropTable", [
            "--tableNameRegex %s" % table_name_regex,
            "--zk %s" % cfg_parser.get("hbase_zk_%s" % label),
            "--versions %s" % str(versions)]
    )
    # logging.info("remove table %s zk %s --version %s" %
    #              (table_name_regex, cfg_parser.get("hbase_zk_%s" % label), str(versions)))


def drop_hbase_table(args):
    if len(args) is 0:
        args = ["-h"]
    parser = argparse.ArgumentParser(prefix_chars='--')
    parser.add_argument('--table_name', help='hbase表名,支持正则,如rp_device_profile_info_[0-9]{8}')
    parser.add_argument('--version_number', default=3, help='保留版本数量 \n 0:全部删除,默认3')
    namespace, other_args = parser.parse_known_args(args=args)
    if other_args or namespace.table_name is None or namespace.table_name is "":
        logging.error('unrecognized arguments: %s' % ' '.join(other_args))
        parser.print_help()

    for label in cfg_parser.get("hbase_cluster").split(","):
        drop_old_version_table(label=label, table_name_regex=namespace.table_name,
                               versions=namespace.version_number)
