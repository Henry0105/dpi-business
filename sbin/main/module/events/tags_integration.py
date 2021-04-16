# coding=utf-8
# 标签

__author__ = 'chenfq'
import logging
import uuid
import shutil
import json


from enum import Enum, unique

from module.const import argparse
from module.const import main_class
from module.rpc import AbstractRpcHandler
from module.tools import *
from module.tools.spark import Spark2
from module.tools.mysql import MySQLClient
from module.tools.cfg import app_utils
from module.tools.env import dataengine_env
from module.tools.opt import OptionParser
from module.tools.dataapi import DataApiOperator
from module.tools.dfs import fast_dfs
from module.tools.http import HttpClient
from module.events.tags import TagsToolHiveGenerator
from module.events.tags import FullTagsGenerator


# http://c.mob.com/pages/viewpage.action?pageId=6654628
@unique
class TagStatus(Enum):
    Pending = 1
    WithDrawn = 2
    Approved = 3
    Rejected = 4
    Processing = 5
    Success = 6
    Failed = 7
    TestPass = 8
    TestFails = 9
    # '状态：1->默认，待审批；2->已撤回；3->审批通过；4->审批未通过；
    # 5->处理中；6->处理成功；7->处理失败；8->测试通过；9->测试未通过'

    @staticmethod
    def wrap2str(arr):
        return [str(x.value) for x in arr]

    # 获取需要导入hbase的标签状态
    @staticmethod
    def get_export_hbase_status():
        return TagStatus.wrap2str([TagStatus.Processing, TagStatus.Success, TagStatus.Failed,
                                   TagStatus.TestPass, TagStatus.TestFails])

    # 获取需要检查hbase的标签状态
    @staticmethod
    def get_check_hbase_status():
        return TagStatus.wrap2str([TagStatus.Processing, TagStatus.Success, TagStatus.Failed, TagStatus.TestFails])

def load_mysql_properties():
    prop = {
        'host': app_utils.get_property("tag.mysql.ip"),
        'port': int(app_utils.get_property("tag.mysql.port")),
        'user': app_utils.get_property("tag.mysql.user"),
        'password': app_utils.get_property("tag.mysql.password"),
        'db': app_utils.get_property("tag.mysql.database")
    }
    return prop


# --update_tag_status --src_status 3 --dest_status 5  状态由 审批通过 到 处理中     #done
# --gen_hive_data --day 20190126 生成hive表新分区的数据  目标表 dm_dataengine_tags.dm_tags_info
# --do_dataapi --create_table --table_name data_api:dm_tags_info_20190126 --regions 512 生成hbase表     #done
# --gen_hfile --day 20190126 --hbase_table data_api:dm_tags_info_20190126 --hbase_cluster test  测试环境   #done
# --check_hbase --day 20190126 --hdfs_output /tmp/dataengine/dm_tags_info_20190126 --hbase_clusters 101 --task_id 123
# --do_dataapi --add_columns --columns c:cateid_12,c:cateid_33 --apiname dm_tags_info_for_test 修改测试的schema    #done
# --do_dataapi --add_columns --columns c:cateid_12,c:cateid_33 --apiname dm_tags_info 修改线上的schema
# --do_dataapi --update_hbase_table_name --table_name dm_tags_info_20190126 --api_name dm_tags_info_for_test 更新测试
# --update_tag_status --src_status 5 --dest_status 6  处理成功
# --call_test_procedure --day 20190227 --dfs_output http://10.21.131.11:20101/fs/download?path=tags_demo_20190227.tar.gz&module=dataengine
# --do_dataapi --update_hbase_table_name --table_name dm_tags_info_20190126 --api_name dm_tags_info 更新正式


class TagsIntegerationTool:
    def __init__(self):
        self.mysql_prop = load_mysql_properties()
        self.dataapi = DataApiOperator()
        self.dfs_root_url = dataengine_env.dfs_root_url
        self.check_hbase_statues = ','.join(TagStatus.get_check_hbase_status())

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--gen_hive_data', action='store_true', default=False, help='生成hive表')
        parser.add_argument('--update_tag_status', action='store_true', default=False, help='修改标签状态')
        parser.add_argument('--do_dataapi', action='store_true', default=False, help='dataapi相关操作')
        parser.add_argument('--gen_hfile', action='store_true', default=False, help='生成hfile')
        parser.add_argument('--check_hbase', action='store_true', default=False, help='hbase数据自查')
        parser.add_argument('--call_test_procedure', action='store_true', default=False, help='调起测试流程')
        parser.add_argument('--init_full_tags', action='store_true', default=False, help='全量标签生成工具')
        parser.add_argument('--update_full_tags', action='store_true', default=False, help='更新全量标签')
        namespace, other_args = parser.parse_known_args(args=args)

        if namespace.gen_hive_data:
            self.gen_hive_data(other_args)
        elif namespace.update_tag_status:
            self.update_tag_status(other_args)
        elif namespace.do_dataapi:
            self.do_dataapi(other_args)
        elif namespace.gen_hfile:
            self.gen_hfile(other_args)
        elif namespace.check_hbase:
            self.check_hbase(other_args)
        elif namespace.call_test_procedure:
            self.call_test_procedure(other_args)
        elif namespace.init_full_tags:
            self.init_full_tags(other_args)
        elif namespace.update_full_tags:
            self.update_full_tags(other_args)
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()

    def init_full_tags(self, args):
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default=False, help='运行时间')
        parser.add_argument('--sample', action='store', default=False, help='是否测试')
        parser.add_argument('--batch', action='store', default=10, help='多少个分区做一次checkpoint')
        namespace, argv = parser.parse_known_args(args=args)

        FullTagsGenerator(namespace.day).init_full_tags(namespace.sample, namespace.batch)

    def update_full_tags(self, args):
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default=False, help='运行日期')
        parser.add_argument('--partitions', action='store', default=8192, help='spark.sql.shuffle.partitions参数')
        parser.add_argument('--memory_overhead', action='store', default='4g', help='spark.executor.memoryOverhead参数')
        namespace, argv = parser.parse_known_args(args=args)

        FullTagsGenerator(namespace.day).update_full_tags(namespace.partitions, namespace.memory_overhead)

    def gen_hive_data(self, args):
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default=False, help='脚本跑的日期')
        parser.add_argument('--sample', action='store', default=False, help='gen_hive_data部分数据')
        parser.add_argument('--batch', action='store', default=10, help='多少个分区做一次checkpoint')
        namespace, argv = parser.parse_known_args(args=args)

        TagsToolHiveGenerator(namespace.day).gen_hive_data(namespace.sample, namespace.batch)

    def update_tag_status(self, args):
        """
        :usage: ./dataengine-tools.sh --tags_integration_tool --update_tag_status --src_status 3 --dest_status 5
        :param args:
        :return:
        """
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--src_status', action='store', default=False, help='标签原始状态')
        parser.add_argument('--dest_status', action='store', default=False, help='标签目标状态')
        namespace, argv = parser.parse_known_args(args=args)
        self.update_tag_status_core(namespace.src_status, namespace.dest_status)

    def do_dataapi(self, args):
        parser = self.dataapi.prepare_parser()
        namespace, other_args = parser.parse_known_args(args=args)
        if namespace.create_table:
            self.dataapi.create_hbase_table(other_args)
        elif namespace.add_columns:
            columns = self.get_columns_to_add()
            new_other_args = other_args.extend(["--columns", ",".join(columns)]) \
                if isinstance(other_args, type(None)) else ["--columns", ",".join(columns)]
            new_other_args.extend(["--apiname", namespace.apiname])
            self.dataapi.add_table_cols(new_other_args)
        elif namespace.update_hbase_table_name:
            self.dataapi.update_hbase_table_name(other_args)
        elif namespace.get_cols:
            self.dataapi.get_hbase_table_cols(namespace.apiname)
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()

    def gen_hfile(self, args):
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default=False, help='脚本跑的日期')
        parser.add_argument('--export_hbase', action='store_true', help='调用导hbase工具导数据')
        parser.add_argument('--hbase_table', action='store', help='hbase表')
        parser.add_argument('--hbase_cluster', action='store', help='hbase集群')
        parser.add_argument('--keep_src_hfile', action='store_true', default=False, help='是否保留生成的hfile')

        namespace, argv = parser.parse_known_args(args=args)

        TagsToolHiveGenerator(namespace.day).export_hbase(namespace.hbase_table, namespace.hbase_cluster,
                                                          namespace.keep_src_hfile)

    def check_hbase(self, args):
        if self.should_skip(self.check_hbase_statues):
            return True
        TagsIntegrationChecker().submit(args)

    def call_test_procedure(self, args):
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default=False, help='默认当前时间')
        parser.add_argument('--test_url', action='store', default=False, help='测试的http接口地址')
        parser.add_argument('--backend_url', action='store', default=False, help='测试的http接口地址')
        namespace, argv = parser.parse_known_args(args=args)

        if self.should_skip(self.check_hbase_statues):  # 直接调用php后端
            logging.info("call backend interface")
            r = HttpClient().post(namespace.backend_url + "?tabName=dm_tags_info_%s" % namespace.day, timeout=10)
            return r.status_code
        else:
            logging.info("call test interface")
            d = json.dumps({
                "filepath": "{dfs_root_url}/fs/download?path=tags_demo_{day}.tar.gz&module=dataengine".format(
                    dfs_root_url=self.dfs_root_url,
                    day=namespace.day),
                "table_name": "dm_tags_info_%s" % namespace.day
            })
            r = HttpClient().post(namespace.test_url, d, timeout=30)
            return r.status_code

    def should_skip(self, status):
        return True # todo 这里的逻辑重新理下
        mysql_client = MySQLClient(**self.mysql_prop)
        profile_ids = TagsIntegerationTool.get_columns_by_status(mysql_client, status)
        return len(profile_ids) < 1

    @staticmethod
    def get_columns_by_status(mysql_client, status):
        id_query = """select c.profile_id
                  from (
                      select id
                      from label_output
                      where status in ({status})) a
                  join label_output_profile b 
                  on a.id = b.output_id
                  join t_profile_version c 
                  on b.profile_version_id = c.id
                  group by c.profile_id
                  """.format(status=status)

        id_rows = mysql_client.query_rows(id_query)
        return [str(row['profile_id']) for row in id_rows]

    @staticmethod
    def get_processing_columns(mysql_client):
        return TagsIntegerationTool.get_columns_by_status(mysql_client, TagStatus.Processing.value)

    @staticmethod
    def get_parent_ids_by_profile_ids(mysql_client, profile_ids):
        if profile_ids is None or len(profile_ids) < 1:
            return []
        else:
            filter_clause = "where profile_id in ({idstr})"\
                .format(idstr=",".join(profile_ids))
            query = """ select c.profile_category_parent_id
                      from (
                           select profile_id, profile_category_id
                           from t_profile_metadata
                           {filter_clause}
                      ) as b
                      inner join t_profile_category as c
                      on b.profile_category_id = c.profile_category_id
                      group by c.profile_category_parent_id
                      """.format(filter_clause = filter_clause)
            rows = mysql_client.query_rows(query)
            return set([row['profile_category_parent_id'] for row in rows])

    def get_columns_to_add(self):
        mysql_client = MySQLClient(**self.mysql_prop)
        profile_ids = TagsIntegerationTool.get_columns_by_status(mysql_client, ','.join(TagStatus.get_check_hbase_status()))
        pids = TagsIntegerationTool.get_parent_ids_by_profile_ids(mysql_client, profile_ids)
        mysql_client.close()
        return ["cateid_{id}".format(id=pid) for pid in pids]

    def update_tag_status_core(self, old_statues, new_status):
        mysql_table_name = "label_output"

        query = """
            UPDATE {mysql_table} SET status = {new_value} WHERE status in ({old_values})
        """.format(mysql_table=mysql_table_name,
                   new_value=int(new_status),
                   old_values=old_statues)

        client = MySQLClient(**self.mysql_prop)
        row_count = client.update_rows(query)
        print (row_count, " rows effected")
        client.close()


class TagsIntegrationChecker(AbstractRpcHandler):
    """
        usage example:
        ./dataengine-tools.sh --tags_check --hbase_clusters 101
        hdfs_output /tmp/dataengine/dm_tags_info_20190213_chenfq  --task_id test_chenfq
    """
    def __init__(self):
        AbstractRpcHandler.__init__(self)
        self.spark2 = Spark2(rpcHandler=self)
        self.job_id = str(uuid.uuid4())
        self.application_id = None
        self.devices = []

    def receive(self, param):
        arr = str.split(str(param), u'\u0001')
        event_code = str(arr[0])
        if event_code == "1":
            # {code}\u0001{application id}
            self.application_id = str(arr[1])
        elif event_code == "2":
            # {device1}\u0002{device2}
            self.devices = str.split(str(arr[1]), u'\u0002')
        else:
            logging.error("rpc handler code[%s] error." % event_code)
            return False
        return True

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default=False, help='默认当前时间')
        parser.add_argument('--hdfs_output', action='store', default=False, help='hdfs 输出路径')
        parser.add_argument('--hbase_clusters', action='store', default=False, help='hbase集群')
        parser.add_argument('--hbase_table_name', action='store', default=False, help='hbase表名')
        parser.add_argument('--sample_num', action='store', default=10, help='采样数目')
        parser.add_argument('--statues', action='store', default=','.join(TagStatus.get_check_hbase_status()),
                            help='需要进行检查的标签状态')
        namespace, other_args = parser.parse_known_args(args=args)

        labels = namespace.hbase_clusters.split(",")
        zk_group = ";".join(
            map(lambda label: cfg_parser.get("hbase_zk_%s" % label), labels)
        )

        job_name = "TagsIntegrationChecker[%s]" % namespace.day
        job_id = self.job_id
        status = self.spark2.submit(
            "dataengine-utils", " ".join([
                "--tableName %s" % namespace.hbase_table_name,
                "--zk '%s'" % zk_group,
                "--rpcHost %s" % self.spark2.rpc_server.host,
                "--rpcPort %s" % self.spark2.rpc_server.port,
                "--day %s" % namespace.day,
                "--hdfsOutput %s" % namespace.hdfs_output,
                "--sampleNum %s" % namespace.sample_num,
                "--statues %s" % namespace.statues
            ]), job_name, job_id,
            props={
                "--class": main_class.TAGS_INTEGRATION_CHECKER,
                "--driver-memory": "4g",
                "--executor-memory": "10g",
                "--executor-cores": "4",
            },
            conf={
                "spark.executor.memoryOverhead": "10240",
                "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors,
            },
            files=['application.properties']
        )

        if status is not 0:
            raise Exception("[TagsIntegrationChecker] %d" % status)

        # 导出dfs
        out = {"value": "tags_demo_%s.tar.gz" % namespace.day}
        target_param = OptionParser.parse_target(out)
        print ("now print target param ", target_param)

        target_dir = "%s/%s" % (dataengine_env.dataengine_data_home, job_id)
        print ("now print target dir ", target_dir)
        import os
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)

        os.makedirs(target_dir)
        os.chdir(target_dir)
        os.makedirs("tags_demo_%s" % namespace.day)

        import subprocess

        p = subprocess.Popen("hdfs dfs -ls %s | awk '{print $8}'" % namespace.hdfs_output,
                             shell = True,
                             stdout = subprocess.PIPE,
                             stderr = subprocess.STDOUT
                             )

        for line in p.stdout.readlines():
            line_strip = line.strip("\n")
            if "task_id" in line:
                psub = subprocess.Popen("hdfs dfs -ls %s/%s | awk '{print $8}'" % (
                    namespace.hdfs_output, line_strip.split("/")[-1]),
                                        shell = True,
                                        stdout = subprocess.PIPE,
                                        stderr = subprocess.STDOUT
                                        )
                for pline in psub.stdout.readlines():
                    pline_strip = pline.strip("\n")
                    task_id_with_split = line_strip.split("/")[-1]
                    task_id = task_id_with_split.split("=")[-1]
                    json_file = pline_strip.split("/")[-1]
                    if len(pline_strip)>0:
                        status = shell.submit("hdfs dfs -mv %s/%s/%s %s/%s/%s.json" % (
                            namespace.hdfs_output,
                            task_id_with_split,
                            json_file,
                            namespace.hdfs_output,
                            task_id_with_split,
                            task_id))
                        if status is not 0:
                            return Exception("hdfs download failed")

        status = shell.submit("hdfs dfs -get %s/*/*.json %s" % (namespace.hdfs_output, "tags_demo_%s" % namespace.day))

        if status is not 0:
            shell.submit("hdfs dfs -rm -r %s/%s" % (namespace.hdfs_output))
            return Exception("hdfs download failed")

        fast_dfs.tgz_upload(target_param)

        shell.submit("hdfs dfs -rm -r %s/" % (namespace.hdfs_output))
        shutil.rmtree("%s/%s" % (dataengine_env.dataengine_data_home, job_id))

        return 0
