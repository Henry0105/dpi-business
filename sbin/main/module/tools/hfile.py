# coding=utf-8
import logging

from module.tools import utils, cfg_parser, hadoop, hdfs
from module.tools.env import dataengine_env
from module.tools.java import Java
from module.tools.utils import Stopwatch


# 根据hash前几位与region数量创建表
class LoadHfile:
    def __init__(self, output, table_name, regions, key_type="byte"):
        self.output = output
        self.table_name = table_name
        self.grant_user = "app,dataengine"
        self.regions = regions
        self.key_type = key_type

    def bulk_load(self, label):
        """
        :param label: 101[test]|120[主]|218[备]
        """
        client = Java(
            jars=["conf/hadoop-conf-%s" % cfg_parser.get("cluster_current_id")] + utils.scan_files(
                dataengine_env.dataengine_home + "/lib/jars"
            )
        )
        hbase_client = Java(
            jars=["conf/hadoop-conf-hbase-%s" % label] + utils.scan_files(
                dataengine_env.dataengine_home + "/lib/jars")
        )

        watcher = Stopwatch()
        logging.info("\n\n%s distcp is beginning %s ..." % (label, watcher.format_current_time()))

        # 1.distcp
        status = hadoop.distcp(self.output, hdfs.get_active_nn(label) + "/" + self.output, "hdfs")
        if status is not 0:
            raise Exception("distcp %s hbase failed" % label)
        logging.info("%s distcp cost %s ." % (label, watcher.watch()))
        logging.info("%s distcp ended %s .\n\n" % (label, watcher.format_current_time()))

        # 2.create hbase table
        client.submit("com.mob.dataengine.utils.hbase.CreateTable", [
            "--keyType %s" % self.key_type,
            "--tableName %s" % self.table_name,
            "--zk %s" % cfg_parser.get("hbase_zk_%s" % label),
            "--regions %s" % self.regions])

        # 3.grant permission
        hbase_client.submit("com.mob.dataengine.utils.hbase.Grant", [
            "-n %s" % self.table_name,
            "-z %s" % cfg_parser.get("hbase_zk_%s" % label),
            "-u %s" % self.grant_user
        ])

        # 4.bulkLoad hfile
        hbase_client.submit("com.mob.dataengine.utils.hbase.BulkLoad", [
            "-n %s" % self.table_name,
            "-z %s" % cfg_parser.get("hbase_zk_%s" % label),
            "-h %s" % self.output
        ])
        logging.info("%s load hfile ended %s ." % (label, watcher.format_current_time()))
        logging.info("%s load hfile cost %s .\n\n\n" % (label, watcher.watch()))


# 根据分区文件创建表
class LoadHfileWithPartitionFile:

    def __init__(self, output, table_name, hbase_partitions_path):
        self.output = output
        self.table_name = table_name
        self.hbase_partitions_path = hbase_partitions_path
        self.grant_user = "app,dataengine"

    def bulk_load(self, label):
        """
        :param label: master|slave
        """

        client = Java(
            jars=["conf/hadoop-conf-%s" % cfg_parser.get("cluster_current_id")] + utils.scan_files(
                dataengine_env.dataengine_home + "/lib/jars"
            )
        )
        hbase_client = Java(
            jars=["conf/hadoop-conf-hbase-%s" % label] + utils.scan_files(
                dataengine_env.dataengine_home + "/lib/jars")
        )

        watcher = Stopwatch()
        logging.info("\n\n%s distcp is beginning %s ..." % (label, watcher.format_current_time()))

        # 1.distcp
        status = hadoop.distcp(self.output, hdfs.get_active_nn(label) + "/" + self.output, "hdfs")
        if status is not 0:
            raise Exception("distcp %s hbase failed" % label)
        logging.info("%s distcp ended %s .\n\n" % (label, watcher.format_current_time()))

        # 2.create hbase table
        client.submit("com.mob.dataengine.utils.hbase.CreateTableWithPartitionFile", [
            "-n %s" % self.table_name,
            "-z %s" % cfg_parser.get("hbase_zk_%s" % label),
            "--partitionsPath %s" % self.hbase_partitions_path])

        # 3.grant permission
        hbase_client.submit("com.mob.dataengine.utils.hbase.Grant", [
            "-n %s" % self.table_name,
            "-z %s" % cfg_parser.get("hbase_zk_%s" % label),
            "-u %s" % self.grant_user
        ])

        # 4.bulkLoad hfile
        hbase_client.submit("com.mob.dataengine.utils.hbase.BulkLoad", [
            "-n %s" % self.table_name,
            "-z %s" % cfg_parser.get("hbase_zk_%s" % label),
            "-h %s" % self.output
        ])
        logging.info("%s load hfile ended %s ." % (label, watcher.format_current_time()))
        logging.info("%s load hfile cost %s .\n\n\n" % (label, watcher.watch()))
