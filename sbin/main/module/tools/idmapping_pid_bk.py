# coding=utf-8
import logging

from module.const.argparse import ArgumentParser
from module.const.main_class import ID_MAPPING_PHONE_BK, ID_MAPPING_PID_BK
from module.tools.spark import Spark2


class IdMappingPhoneBk:
    def __init__(self, args):
        self.spark2 = Spark2(rpcHandler=self)
        self.spark_args, other_args, self.day = self.get_spark_args(args)
        self.props, self.conf = self.get_spark_props(other_args)

    @staticmethod
    def get_spark_args(args):
        parser = ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default='', help='day 例如:20180806')
        parser.add_argument('--full', action='store', default='false', help='是否是全量生成')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark args:')
        logging.info(namespace)
        logging.info(other_args)

        # 参数解析完转换成list,sql需要单独处理下
        spark_args = """ --day {day} \\
        --full {full} """.format(
            day=namespace.day,
            full=namespace.full
        )
        return spark_args, other_args, namespace.day

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
        parser.add_argument('--executor_cores', action='store', default="3", help='spark executor核数')
        parser.add_argument('--executor_memory', action='store', default="9G", help='spark executor内存')
        # conf部分
        parser.add_argument('--conf', action='store', nargs='+', help='spark-submit --config会取代默认的')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark conf and props:')
        logging.info(namespace)
        logging.info(other_args)

        props = {
            "--class": ID_MAPPING_PHONE_BK,
            "--driver-memory": namespace.driver_memory,
            # "--driver-cores": namespace.driver_cores,
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
            job_name='idmapping_phone_bk',
            job_id=self.day,
            props=self.props,
            conf=self.conf
        )


class IdMappingPidBk:
    def __init__(self, args):
        self.spark2 = Spark2(rpcHandler=self)
        self.spark_args, other_args, self.day = self.get_spark_args(args)
        self.props, self.conf = self.get_spark_props(other_args)

    @staticmethod
    def get_spark_args(args):
        parser = ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default='', help='day 例如:20180806')
        parser.add_argument('--full', action='store', default='false', help='是否是全量生成')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark args:')
        logging.info(namespace)
        logging.info(other_args)

        # 参数解析完转换成list,sql需要单独处理下
        spark_args = """ --day {day} \\
        --full {full} """.format(
            day=namespace.day,
            full=namespace.full
        )
        return spark_args, other_args, namespace.day

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
        parser.add_argument('--executor_cores', action='store', default="3", help='spark executor核数')
        parser.add_argument('--executor_memory', action='store', default="9G", help='spark executor内存')
        # conf部分
        parser.add_argument('--conf', action='store', nargs='+', help='spark-submit --config会取代默认的')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark conf and props:')
        logging.info(namespace)
        logging.info(other_args)

        props = {
            "--class": ID_MAPPING_PID_BK,
            "--driver-memory": namespace.driver_memory,
            # "--driver-cores": namespace.driver_cores,
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
            job_name='idmapping_phone_bk',
            job_id=self.day,
            props=self.props,
            conf=self.conf
        )
