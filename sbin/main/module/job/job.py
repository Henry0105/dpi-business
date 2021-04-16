# coding=utf-8
import shutil
import os
import logging
import traceback

from module.rpc import AbstractRpcHandler
from module.job.job_msg import JobMsg
from module.job.error import DataEngineException
from module.tools.spark import Spark2
from module.tools.env import dataengine_env
from module.tools import shell
from module.tools.hive import Hive
from module.tools import utils
from module.tools.cfg import prop_utils
from module.tools.decorators import skip_test
from module.tools.spark_sql import SparkSQL


class Job:
    """
    每个任务:
    接收一个参数,将种子数据都落到hive表
    解析出需要/添加的参数,传给spark去执行,期间会接收spark任务的返回信息,存储起来
    拉取数据包,对数据包进行处理并发送到dfs
    """

    def __init__(self, job):
        self.job_msg = JobMsg()
        self.job = job
        self.job_id = str(self.job['job_id']) if 'job_id' in self.job else str(self.job['jobId'])
        self.job_name = str(self.job['job_name']) if 'job_name' in self.job else str(self.job['jobName'])
        self.job_msg.put('job_id', self.job_id)
        self.job_msg.put('job_name', self.job_name)
        self.exception = None
        self.outputs = None
        self.queue = self.dispatch_queue()

    def validate(self):
        """
        参数校验
        :return:
        """
        pass

    def prepare(self):
        pass

    def submit(self):
        functions = [self.validate, self.prepare, self.run, self.upload, self.clean_up]
        for fn in functions:
            self.safe_fn(fn)

        if self.has_error():
            self.fill_error()
            return JobStatus(1, self.job_msg, self.exception)
        else:
            return JobStatus(0, self.job_msg, self.exception)

    def run(self):
        """
        提交spark任务
        :return:
        """
        pass

    def upload(self):
        """
        上传数据到dfs
        :return:
        """
        pass

    @skip_test
    def clean_up(self):
        """
        清理临时数据目录
        :return:
        """
        shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
        os.chdir(dataengine_env.dataengine_data_home)
        shutil.rmtree("%s/%s" % (dataengine_env.dataengine_data_home, self.job_id), ignore_errors=True)

    def safe_fn(self, fn):
        if self.has_error():
            return False
        try:
            fn()
            return True
        except DataEngineException, e:
            self.exception = e
            logging.error("dataengine exception stack: %s " % traceback.format_exc(1000))
        except Exception, e:
            self.exception = DataEngineException(e.message)
            logging.error("exception stack: %s " % traceback.format_exc(1000))

        return False

    def has_error(self):
        return self.exception is not None

    def fill_error(self):
        """
        填充错误信息,在此阶段需要获取到spark的错误信息
        """
        if self.has_error():
            self.get_spark_error()
            self.job_msg.merge(self.exception.to_dict())

    def get_spark_error(self):
        if self.job_msg.get('application_id') is not None:
            diagnostics = Spark2.get_job_diagnostics(self.job_msg.get('application_id'))
            if not self.job_msg.is_empty() and len(diagnostics) > 0:
                self.exception = DataEngineException(diagnostics)

    def skip_test(job):
        return 'local' == os.environ['MID_ENGINE_ENV'] or 'test' in job

    def dispatch_queue(self):
        user = None
        if self.job_name == "profile_export_back_track":
            user = "mobfin"
        elif self.job_name == 'pid_operator_mapping':
            user = 'datax'
        elif self.job_name in ['carrier_profile', 'jiguang_profile', 'getui_profile']:
            user = 'datax'
        elif self.job_name == 'id_mapping_v4':
            if 'ad_marketing' in self.job['projectName']:
                user = 'datax'
            elif 'mobfin_premininglab' in self.job['projectName']:
                user = 'mobfin'
            else:
                pass
        queue = Job.format_queue(user)
        print("dispath to %s" % queue)
        return queue

    @staticmethod
    def format_queue(user):
        if user is None:
            return None
        else:
            suffix = '' if dataengine_env.env_scope == 'prod' else "_test"
            return "root.yarn_{user}.{user}{suffix}".format(user=user, suffix=suffix)

class RpcJob(Job, AbstractRpcHandler):
    def __init__(self, job):
        Job.__init__(self, job)
        AbstractRpcHandler.__init__(self)
        self.spark2 = Spark2(rpcHandler=self, queue=self.queue)
        self.hive = Hive()
        self.sparkSQL = SparkSQL(self.queue)
        self.temp_table = '{db}.dataengine_tmp_opt_table'.format(db=dataengine_env.dataengine_db_name)

    def prepare(self):
        Job.prepare(self)
        self.job['rpc_host'] = self.spark2.rpc_server.host
        self.job['rpc_port'] = self.spark2.rpc_server.port
        self.job['day'] = str(self.job['day']) if 'day' in self.job else str(utils.current_day())

    def receive(self, rpc_param):
        self.store_application_id(rpc_param)
        return True

    def persist_to_opt_table(self, filename, uuid):
        self.sparkSQL.submit("""
            load data local inpath '{filename}' OVERWRITE into table {tmp_table}
              partition(day='{day}', uuid='{uuid}');
            insert overwrite table {opt_table}
                partition(created_day='{day}', biz='from_dfs', uuid='{uuid}')
            select data from {tmp_table}
            where uuid = '{uuid}';
        """.format(tmp_table=self.temp_table, opt_table=prop_utils.HIVE_TABLE_DATA_OPT_CACHE,
                   filename=filename, day=self.job['day'], uuid=uuid))

    def persist_to_data_hub_table(self, filename, uuid):
        self.sparkSQL.submit("""
            load data local inpath '{filename}' OVERWRITE into table {tmp_table}
              partition(day='{day}', uuid='{uuid}');
            insert overwrite table {opt_table}
                partition(uuid='{uuid}')
            select map('seed', array(data)) from {tmp_table}
            where uuid = '{uuid}';
        """.format(tmp_table=self.temp_table, opt_table=prop_utils.HIVE_TABLE_DATA_HUB,
                   filename=filename, uuid=uuid, day=self.job['day']))

    def persist_to_opt_table_from_sql(self, sql_clause, uuid):
        self.sparkSQL.submit("""
            insert overwrite table {opt_table}
                partition(created_day='{day}', biz='from_sql', uuid='{uuid}')
            select device from (
                {sql_clause}
            ) tmp;
        """.format(opt_table=prop_utils.HIVE_TABLE_DATA_OPT_CACHE,
                   day=self.job['day'], uuid=uuid, sql_clause=sql_clause))

    def persist_to_opt_new_table(self, filename, uuid):
        self.sparkSQL.submit("""
            load data local inpath '{filename}' OVERWRITE into table {tmp_table}
              partition(day='{day}', uuid='{uuid}') ;
            insert overwrite table {opt_table}
                partition(day='{day}', uuid='{uuid}')
            select null as id, map(4, device) as match_ids, null as id_type, 
                null as encrypt_type, data from (
                select split(data, ',')[0] device, data from {tmp_table}
                where uuid = '{uuid}'
                ) as a;
        """.format(tmp_table=self.temp_table, opt_table=prop_utils.HIVE_TABLE_DATA_OPT_CACHE_NEW,
                   filename=filename, day=self.job['day'], uuid=uuid))

    @staticmethod
    def extract_code_and_rest(rpc_param):
        """
        拆分出event_code和后面的信息
        :param rpc_param:
        :return: event_code, rest(其他信息)
        """
        print('job receive:', rpc_param)
        arr = str.split(str(rpc_param), u'\u0001')
        return str(arr[0]), str(arr[1])

    def store_application_id(self, rpc_param):
        """
        保存任务的application_id
        :param rpc_param:
        :return:
        """
        event_code, rest = RpcJob.extract_code_and_rest(rpc_param)
        if event_code == "1":
            # {code}\u0001{application id}
            self.job_msg.put('application_id', rest)


class JobStatus:
    def __init__(self, status, job_msg, exception):
        self.status = status
        self.job_msg = job_msg
        self.exception = exception
