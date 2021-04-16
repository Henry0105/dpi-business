# encoding:utf-8
__author__ = 'zhangjt'
import uuid

from module.const import argparse
from module.const.main_class import TAGS_HFILE_GENERATOR, TAGS_V2_GENERATOR
from module.rpc import AbstractRpcHandler
from module.tools.env import dataengine_env
from module.tools.spark import Spark2


class TagsHFile(AbstractRpcHandler):
    def __init__(self):
        AbstractRpcHandler.__init__(self)
        self.spark2 = Spark2(rpcHandler=self)
        self.application_id = None
        self.job_id = str(uuid.uuid4())

    def gen_hive_data(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--par', action='store', help='全量表日期')
        parser.add_argument('--end_day', action='store', help='回溯结束日期')
        parser.add_argument('--span', action='store', help='往前推几天')
        parser.add_argument('--partition', action='store', default="10000", help='回溯结束日期')

        namespace, other_args = parser.parse_known_args(args=args)

        conf = {
            "spark.speculation.quantile": "0.99",
            "spark.sql.shuffle.partitions": namespace.partition,
            "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors,
        }

        props = {
            "--class": TAGS_V2_GENERATOR,
            "--executor-memory": "9g",
            "--executor-cores": "3"
        }

        return self.spark2.submit(
            module="dataengine-utils",
            args=""" \\
            --par {par} \\
            --endDay {end_day} \\
            --span {span}
            """.format(
                par=namespace.par,
                end_day=namespace.end_day, span=namespace.span
            ),
            job_name='tags_v2_hive_generator',
            job_id=self.job_id,
            props=props,
            conf=conf,
            files=["application.properties"]
        )

    def gen_hfile(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--zk', action='store', help='zk地址')
        parser.add_argument('--row_key', action='store', help='row key')
        parser.add_argument('--hdfs_path', action='store', help='hdfs路径')
        parser.add_argument('--prefix', action='store', default="c_", help='hbase列名前缀')
        parser.add_argument('--num', action='store', default="1024", help='hfile的个数')
        parser.add_argument('--end_day', action='store', help='增量表结束日期')
        parser.add_argument('--span', action='store', default='0', help='往前推几天')
        parser.add_argument('--partition', action='store', default='1024', help='增量表日期')

        namespace, other_args = parser.parse_known_args(args=args)

        conf = {
            "spark.speculation.quantile": "0.99",
            "spark.executor.userClassPathFirst": "true",
            "spark.sql.shuffle.partitions": namespace.partition,
            "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors,
        }
        props = {
            "--class": TAGS_HFILE_GENERATOR,
            "--executor-memory": "9g",
            "--executor-cores": "3"
        }

        return self.spark2.submit(
            module="dataengine-utils",
            args=""" \\
            --zk {zk} \\
            --rowKey {row_key} \\
            --hdfsPath {hdfs_path} \\
            --endDay {end_day} \\
            --span {span} \\
            --prefix {prefix} \\
            --partitions {num}
            """.format(
                zk=namespace.zk, row_key=namespace.row_key,
                hdfs_path=namespace.hdfs_path, end_day=namespace.end_day, span=namespace.span,
                prefix=namespace.prefix, num=namespace.num
            ),
            job_name='tags_hfile_generator',
            job_id=self.job_id,
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

