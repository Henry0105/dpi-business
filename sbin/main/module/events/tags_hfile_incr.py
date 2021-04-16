# encoding:utf-8

import os
import uuid

from module.const import argparse
from module.const.main_class import TAGS_HFILE_GENERATOR_BOOTSTRAP
from module.rpc import AbstractRpcHandler
from module.tools.env import dataengine_env
from module.tools.spark import Spark2


class TagsHFileIncr(AbstractRpcHandler):
    def __init__(self):
        AbstractRpcHandler.__init__(self)
        self.spark2 = Spark2(rpcHandler=self)
        self.application_id = None
        self.job_id = str(uuid.uuid4())

    def gen_hfile(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--os', action='store', default="android", help='安卓 or ios')
        parser.add_argument('--zk', action='store', help='zk地址')
        parser.add_argument('--row_key', action='store', help='row key')
        parser.add_argument('--hdfs_path', action='store', help='hdfs路径')
        parser.add_argument('--prefix', action='store', default="c_", help='hbase列名前缀')
        parser.add_argument('--num', action='store', default="1024", help='hfile的个数')
        parser.add_argument('--day', action='store', help='增量表结束日期')
        parser.add_argument('--partition', action='store', default="8192", help='shuffle 分区数')
        parser.add_argument('--full', action='store', default="false", help='是否生成全量表')
        parser.add_argument('--executor_memory', action='store', default="9g", help='executor 内存')
        parser.add_argument('--executor_cores', action='store', default="3", help='executor cpu数目')

        namespace, other_args = parser.parse_known_args(args=args)

        conf = {
            "spark.speculation.quantile": "0.99",
            "spark.executor.userClassPathFirst": "true",
            "spark.sql.shuffle.partitions": namespace.partition,
            "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors,
        }
        props = {
            "--class": TAGS_HFILE_GENERATOR_BOOTSTRAP,
            "--executor-memory": namespace.executor_memory,
            "--executor-cores": namespace.executor_cores
        }

        os.system("hdfs dfs -rm -r -f {hdfsPath}".format(hdfsPath=namespace.hdfs_path))

        return self.spark2.submit(
            module="dataengine-utils",
            args=""" \\
            --os {os} \\
            --zk {zk} \\
            --rowKey {row_key} \\
            --hdfsPath {hdfs_path} \\
            --day {day} \\
            --prefix {prefix} \\
            --partitions {num} \\
            --full {full}
            """.format(
                os=namespace.os, zk=namespace.zk,
                row_key=namespace.row_key, hdfs_path=namespace.hdfs_path,
                day=namespace.day, prefix=namespace.prefix,
                num=namespace.num, full=namespace.full
            ),
            job_name='{0}_tags_info_hfile_generator_incr'.format(namespace.os),
            job_id=namespace.day,
            props=props,
            conf=conf,
            files=["application.properties"],
            jars=["hash_unsafe.jar",
                  "jars/metrics-core-2.2.0.jar",
                  "jars/hbase-annotations-1.2.0-cdh5.7.6.jar",
                  "jars/hbase-client-1.2.0-cdh5.7.6.jar",
                  "jars/hbase-common-1.2.0-cdh5.7.6.jar",
                  "jars/hbase-common-1.2.0-cdh5.7.6-tests.jar",
                  "jars/hbase-hadoop2-compat-1.2.0-cdh5.7.6.jar",
                  "jars/hbase-hadoop-compat-1.2.0-cdh5.7.6.jar",
                  "jars/hbase-prefix-tree-1.2.0-cdh5.7.6.jar",
                  "jars/hbase-procedure-1.2.0-cdh5.7.6.jar",
                  "jars/hbase-protocol-1.2.0-cdh5.7.6.jar",
                  "jars/hbase-server-1.2.0-cdh5.7.6.jar"]
        )
