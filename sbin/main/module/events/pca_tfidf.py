# coding=utf-8
import logging
import datetime
import uuid
from module.const import argparse
from module.tools.hive import Hive
from module.tools.cfg import prop_utils
from module.tools.spark import Spark2
from module.tools.utils import Stopwatch


class PCATfidf:
    def __init__(self, args):
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', help='日期')
        namespace, other_args = parser.parse_known_args(args=args)
        if other_args:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()

        self.day = namespace.day
        self.date_fm = '%Y%m%d'
        if self.day is None:
            self.day = (datetime.datetime.now() - datetime.timedelta(2)).strftime(self.date_fm)
        self.job_name = "PCATfidf[%s]" % self.day
        self.job_id = str(uuid.uuid4())

    def submit(self):
        hive = Hive()
        day1 = hive.latest_partition(prop_utils.get_property("table_rp_mobeye_tfidf_pca_tags_mapping")).split('=')[1]
        day60 = (datetime.datetime.strptime(self.day, self.date_fm) - datetime.timedelta(60)).strftime(self.date_fm)

        spark2 = Spark2()
        watch = Stopwatch()

        args = "%s %s %s %s" % (self.day, day1, day60, '/user/dataengine/lookalike/pc_201706.csv')

        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.PCATfidf",
                "--driver-memory": "8g",
                "--executor-memory": "20g",
                "--executor-cores": "4",
            },
            conf={
                "spark.sql.shuffle.partitions": "5000",
                "spark.storage.memoryFraction": "0.1",
                "spark.shuffle.memoryFraction": "0.8",
                "spark.memory.useLegacyMode": "true"
            })

        logging.info("%s is ended %s ." % (self.job_name, watch.format_current_time()))
        logging.info("%s cost %s .\n\n\n" % (self.job_name, watch.watch()))

        return 0

    def submit2(self):
        spark2 = Spark2()
        watch = Stopwatch()

        args = ""

        spark2.submit(
            "dataengine-utils", args, self.job_name, self.job_id,
            props={
                "--class": "com.mob.dataengine.utils.featurePrepare",
                "--driver-memory": "8g",
                "--executor-memory": "20g",
                "--executor-cores": "4",
            },
            conf={
                "spark.sql.shuffle.partitions": "5000",
                "spark.executor.memoryOverhead": "10240",
                "spark.storage.memoryFraction": "0.1",
                "spark.shuffle.memoryFraction": "0.8",
                "spark.memory.useLegacyMode": "true"
            })

        logging.info("%s is ended %s ." % (self.job_name, watch.format_current_time()))
        logging.info("%s cost %s .\n\n\n" % (self.job_name, watch.watch()))

        return 0