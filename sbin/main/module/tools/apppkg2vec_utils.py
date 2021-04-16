# coding=utf-8
import logging
import uuid

from module.tools.env import dataengine_env
from module.tools.spark import Spark2
from module.const.argparse import ArgumentParser
from module.tools.utils import Stopwatch

__author__ = 'sunyl'


class Apppkg2Vec:
    def __init__(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = ArgumentParser()
        parser.add_argument('-d', '--day', action='store', default=False, help='计算日期')
        parser.add_argument('-c', '--cnt', action='store', default=50, help='app2vec过滤阈值')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info(namespace)
        logging.info(other_args)

        self.day = '%s01' % namespace.day[0:6]
        self.cnt = int(namespace.cnt)
        self.job_name = "Apppkg2Vec[%s]" % self.day
        self.job_id = str(uuid.uuid4())
        self.spark2 = Spark2()

    def preparation_cal(self):
        """
        app2vec|icon2vec|detail2vec 特征计算与入库
        """
        watch = Stopwatch()
        job_name = '%s_preparation_cal' % self.job_name

        self.spark2.submit(
            'dataengine-utils',
            args=' --day %s --cnt %d' % (self.day, self.cnt),
            job_name=job_name,
            job_id=self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.apppkg2vec.Apppkg2VectorPreparation",
                "--driver-memory": "8g",
                "--executor-memory": "15g",
                "--executor-cores": "5",
                "--num-executors": "40"
            },
            conf={
                "spark.yarn.executor.memoryOverhead": "4096",
                "spark.dynamicAllocation.enabled": "true",
                "spark.kryoserializer.buffer.max": "2047m",
                "spark.driver.maxResultSize": "4g"
            },
            files=[
                "stopWords.dic"
            ]
        )

        logging.info("%s is ended %s ." % (job_name, watch.format_current_time()))
        logging.info("%s cost %s .\n\n\n" % (job_name, watch.watch()))

        return 0

    def top_similarity_cal(self):
        """
        全量app相似度预计算
        """
        watch = Stopwatch()
        job_name = '%s_top_similarity_cal' % self.job_name

        self.spark2.submit(
            'dataengine-utils',
            args=' --day %s' % self.day,
            job_name=job_name,
            job_id=self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.apppkg2vec.ApppkgTopSimilarity",
                "--driver-memory": "8g",
                "--executor-memory": "15g",
                "--executor-cores": "3",
                "--num-executors": "100"
            },
            conf={
                "spark.yarn.executor.memoryOverhead": "4096",
                "spark.dynamicAllocation.enabled": "true",
                "spark.kryoserializer.buffer.max": "2047m",
                "spark.driver.maxResultSize": "4g",
                "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors
            }
        )

        logging.info("%s is ended %s ." % (job_name, watch.format_current_time()))
        logging.info("%s cost %s .\n\n\n" % (job_name, watch.watch()))

        return 0
