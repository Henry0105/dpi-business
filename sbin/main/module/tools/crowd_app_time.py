# encoding:utf-8
import logging

from module.const import argparse, main_class

from module.const.argparse import ArgumentParser
from module.tools.spark import Spark2


class CrowdAppTime:
    def __init__(self, args):
        self.spark2 = Spark2(rpcHandler=self)
        self.spark_args, self.day, other_args2 = self.get_spark_args(args)
        self.props, self.conf = self.get_spark_props(other_args2)

    @staticmethod
    def get_spark_args(args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', help='任务日期')
        parser.add_argument('--full', action='store', help='是否生成全量表')
        parser.add_argument('--num', action='store', help='生成月份数量')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark args:')
        logging.info(namespace)
        logging.info(other_args)

        ns_dict = namespace.__dict__
        raw_spark_args = list(
            map(lambda x: "--{0} {1}".format(x, ns_dict[x]), filter(lambda x: ns_dict[x] is not None, ns_dict))
        )
        spark_args = " ".join(raw_spark_args)
        return spark_args, namespace.day, other_args

    def get_spark_props(self, args):
        """
        解析spark-submit的配置参数,未配置则采用默认参数
        :return: props  spark-submit props部分
        :return: conf   spark-submit conf部分
        """
        # props部分
        parser = ArgumentParser(prefix_chars='--')
        parser.add_argument('--driver_cores', action='store', default="3", help='spark driver核数')
        parser.add_argument('--driver_memory', action='store', default="9G", help='spark driver内存')
        parser.add_argument('--executor_cores', action='store', default="4", help='spark executor核数')
        parser.add_argument('--executor_memory', action='store', default="10G", help='spark executor内存')
        # conf部分
        parser.add_argument('--conf', action='store', nargs='+', help='spark-submit --config会取代默认的')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark conf and props:')
        logging.info(namespace)
        logging.info(other_args)

        props = {
            "--class": main_class.CrowdAppTime,
            "--driver-memory": namespace.driver_memory,
            "--driver-cores": namespace.driver_cores,
            "--executor-memory": namespace.executor_memory,
            "--executor-cores": namespace.executor_cores
        }
        conf = dict()
        if namespace.conf:
            for item in namespace.conf:
                arr = item.split('=')
                conf[arr[0]] = arr[1]

        return props, conf

    def submit(self):
        logging.info(self.spark_args)
        self.spark2.submit(
            "dataengine-utils",
            args=self.spark_args,
            job_name='CrowdAppTimeGenerator',
            job_id=self.day,
            props=self.props,
            conf=self.conf,
            files=["application.properties"]
        )
