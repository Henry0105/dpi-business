# encoding:utf-8
import logging

from module.const import argparse

from module.const.argparse import ArgumentParser
import module.const.main_class as mc
from module.tools.spark import Spark2

class IosTagsGeneratorSec:
    def __init__(self, args):
        self.spark2 = Spark2(rpcHandler=self)
        self.job_name, self.class_name, other_args1 = self.divide_by_step(args)
        self.spark_args, self.day, other_args2 = self.get_spark_args(other_args1)
        self.props, self.conf = self.get_spark_props(other_args2)

    @staticmethod
    def divide_by_step(args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--step', action='store', type=int,
                            help='1:IosTagsChecker  2:IosTagsGeneratorIncr  3:IosTagsGeneratorFull')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('step:')
        logging.info(namespace)
        logging.info(other_args)

        job_names = {
            1: "IosTagsCheckerSec",
            2: "IosTagsGeneratorIncrSec",
            3: "IosTagsGeneratorFullSec"
        }
        classes = {
            1: mc.TAGS_PROFILE_CHECKER,
            2: mc.TAGS_PROFILE_INFO_GENERATOR_INCR,
            3: mc.TAGS_PROFILE_INFO_GENERATOR_FULL
        }
        job_name = job_names[namespace.step]
        class_name = classes[namespace.step]
        return job_name, class_name, other_args

    @staticmethod
    def get_spark_args(args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', help='任务日期')
        parser.add_argument('--sample', action='store', help='是否取样')
        parser.add_argument('--batch', action='store', help='批次处理大小')
        parser.add_argument('--full', action='store', help='是否生成全量表')

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
        parser.add_argument('--driver_memory', action='store', default="6G", help='spark driver内存')
        parser.add_argument('--executor_cores', action='store', default="4", help='spark executor核数')
        parser.add_argument('--executor_memory', action='store', default="8G", help='spark executor内存')
        # conf部分
        parser.add_argument('--conf', action='store', nargs='+', help='spark-submit --config会取代默认的')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark conf and props:')
        logging.info(namespace)
        logging.info(other_args)

        props = {
            "--class": self.class_name,
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
            job_name=self.job_name,
            job_id=self.day,
            props=self.props,
            conf=self.conf,
            files=["application.properties"]
        )