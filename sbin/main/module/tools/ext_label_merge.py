# coding=utf-8
import logging
import uuid

from module.const.argparse import ArgumentParser
from module.tools.spark import Spark2


class ExtLabelMerge:
    def __init__(self, args):
        self.spark2 = Spark2(rpcHandler=self)
        self.spark_args, other_args = self.get_spark_args(args)
        self.props, self.conf = self.get_spark_props(other_args)

    @staticmethod
    def get_spark_args(args):
        if len(args) is 0:
            args = ["-h"]
        parser = ArgumentParser()
        parser.add_argument('-d', '--day', action='store', default=False, help='计算日期')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark args:')
        logging.info(namespace)
        logging.info(other_args)

        spark_args = """  --day {day} """.format(day=namespace.day)

        return spark_args, other_args

    @staticmethod
    def get_spark_props(args):
        """
        解析spark-submit的配置参数,未配置则采用默认参数
        :return: props  spark-submit props部分
        :return: conf   spark-submit conf部分
        """
        # props部分
        parser = ArgumentParser(prefix_chars='--')
        # parser.add_argument('--driver_cores', action='store', default="3", help='spark driver核数')
        parser.add_argument('--driver_memory', action='store', default="6G", help='spark driver内存')
        parser.add_argument('--executor_cores', action='store', default="4", help='spark executor核数')
        parser.add_argument('--executor_memory', action='store', default="8G", help='spark executor内存')
        # conf部分
        parser.add_argument('--config', action='store', nargs='+', help='spark-submit --config会取代默认的')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark conf and props:')
        logging.info(namespace)
        logging.info(other_args)

        props = {
            "--class": "com.mob.dataengine.utils.tags.profile.ExtLableRelation",
            "--driver-memory": namespace.driver_memory,
            # "--driver-cores": namespace.driver_cores,
            "--executor-memory": namespace.executor_memory,
            "--executor-cores": namespace.executor_cores
        }
        conf = dict()
        if conf:
            for item in namespace.config:
                arr = item.split('=')
                conf[arr[0]] = arr[1]

        return props, conf

    def submit(self):
        logging.info(self.spark_args)
        self.spark2.submit(
            "dataengine-utils",
            args=self.spark_args,
            job_name='ext_lable_merge',
            job_id=str(uuid.uuid4()),
            props=self.props,
            conf=self.conf,
            files={"application.properties"}
        )
