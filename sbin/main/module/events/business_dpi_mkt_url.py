import json
import os
import shutil
import uuid

from module.const import main_class
from module.job.job import RpcJob

__author__ = 'xlmeng'

from module.tools import shell

from module.tools.cfg import prop_utils, app_utils
from module.tools.check_cont import CheckCont, CheckType
from module.tools.decorators import skip_test
from module.tools.dfs import fast_dfs
from module.tools.dir_context import DirContext
from module.tools.env import dataengine_env

from module.tools.notify import MQ


class BusinessDpiMktUrl(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.mq = MQ()
        self.file_name = ''
        self.hdfs_path = 'hdfs://{0}/business/dpi'.format(app_utils.get_property("dataengine.hdfs.tmp"))

    def receive(self, rpc_param):
        RpcJob.receive(self, rpc_param)
        event_code, rest = RpcJob.extract_code_and_rest(rpc_param)
        if event_code == "2":
            # {code}\u0001{uuid}\u0002{count}
            _, match_info = str.split(rest, u'\u0002')
            self.job_msg.put_into_details(json.loads(match_info))
        return True

    def validate(self):
        c = CheckCont(self.job['params'])
        with c as (inputs, output):
            c.check_inputs('value', [CheckType.EXISTS, CheckType.NON_EMPYT])
            c.check_inputs('uuid', [CheckType.EXISTS, CheckType.NON_EMPYT])
            c.check_inputs('carriers', [CheckType.EXISTS, CheckType.NON_EMPYT])

    def prepare(self):
        RpcJob.prepare(self)
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id

        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                for _input in param['inputs']:
                    _input['value'] = _input.get('value')
                    self.file_name = _input.get('uuid')
                    file_uuid = str(uuid.uuid4())
                    fast_dfs.download_with_decrypt_with_input_v2(_input, file_uuid)
                    os.system('hdfs dfs -rm -r -f {0}/{1}'.format(self.hdfs_path, self.file_name))
                    os.system('hdfs dfs -mkdir -p {0}/{1}'.format(self.hdfs_path, self.file_name))
                    os.system('hdfs dfs -put {0} {1}/{2}/'.format(file_uuid, self.hdfs_path, self.file_name))
                    _input['url'] = '{0}/{1}'.format(self.hdfs_path, self.file_name)

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--queue": app_utils.get_property("queue.dpi"),
                "--class": main_class.BUSINESS_DPI_MKT_URL,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3"
            },
            conf={
                "spark.dynamicAllocation.maxExecutors": str(max([int(dataengine_env.max_executors), 200])),
                "spark.kryoserializer.buffer.max": "512m",
                "spark.executor.memoryOverhead": "3g"
            },
            files=["application.properties", "business/dpi/*.sql"],
            hdfs_jars={
                'hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar',
                'hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar'
            })

    @skip_test
    def clean_up(self):
        shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
        os.chdir(dataengine_env.dataengine_data_home)
        shutil.rmtree("%s/%s" % (dataengine_env.dataengine_data_home, self.job_id), ignore_errors=True)
        os.system('hdfs dfs -rm -r -f {0}/{1}'.format(self.hdfs_path, self.file_name))
