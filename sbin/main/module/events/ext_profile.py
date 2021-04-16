# coding=utf-8

import uuid
import json
import os
import shutil
from module import tools
from module.tools.cfg import prop_utils
from module.tools.check_cont import CheckCont, CheckType
from module.tools.env import dataengine_env
from module.tools.notify import MQ
from module.const import main_class
from module.tools.opt import OptionParser
from module.tools import shell
from module.tools.dfs import fast_dfs
from module.tools import utils
from module.tools.utils import get_limit
from module.tools.dir_context import DirContext
from module.job.job import RpcJob
from module.job.error import DataEngineException

__author__ = 'nn'


class ExternalProfile(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.dispatch = {
            'getui_profile': main_class.GETUI_PROFILE,
            'carrier_profile': main_class.CARRIER_PROFILE,
            'jiguang_profile': main_class.JIGUANT_PROFILE
        }
        self.klass = self.dispatch[job['jobName']]

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
            # 参数检查
            # 任务里面要传种子包的验证value是存在的
            c.check_inputs('value', [CheckType.EXISTS, CheckType.NON_EMPYT])
            # 验证profile_ids存在并且不为空数组
            c.check_inputs('profileIds', [CheckType.EXISTS, CheckType.NON_EMPYT])

    def prepare(self):
        RpcJob.prepare(self)
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id

        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                inputs_len = len(param['inputs'])
                profile_ids = []
                for input_idx, input in enumerate(param['inputs']):
                    input['header'] = input.get('header', 0)
                    if 'inputType' not in input or (input['inputType'] != 'uuid' and input['inputType'] != 'sql'):
                        file_uuid = str(uuid.uuid4())
                        fast_dfs.download_with_decrypt_with_input_v2(input, file_uuid)
                        self.persist_to_data_hub_table(file_uuid, file_uuid)
                        input['uuid'] = file_uuid
                        # 对传入的header加前缀,以防重复
                        headers_str = self.inference_headers(file_uuid)
                        if input['header'] > 0:
                            if 'sep' in input:
                                input['headers'] = ['in_%s' % h for h in headers_str.split(str(input['sep']))]
                            else:  # 只有1列数据
                                input['sep'] = ','
                                input['idx'] = 1
                                input['headers'] = ['in_%s' % headers_str]
                        elif 'sep' in input:  # 没有header,但是有分割符,此时手动添加headers
                            input['header'] = 1
                            input['headers'] = ['in_%d' % i for i in range(len(headers_str.split(str(input['sep']))))]
                        else:
                            pass
                    elif input['inputType'] == 'uuid':
                        input['uuid'] = input['value']
                    elif input['inputType'] == 'sql':
                        # 输入格式为sql的时候, input['uuid']为sql语句,需要进行转义
                        input['uuid'] = input['value'].replace('\'', '@')
                        input['uuid'] = input['uuid'].replace('\"', '@')
                        input['value'] = None

                    profile_ids += list(set(input['profileIds']) - set(profile_ids))
                    input['profileIds'] = [] if input_idx < inputs_len - 1 else profile_ids

        for idx, param in enumerate(self.job['params']):
            out = param['output']
            out['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            out["limit"] = get_limit(out)

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": self.klass,
                "--driver-memory": "10g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf={
                "spark.driver.maxResultSize": "3G",
                "spark.kryoserializer.buffer.max": "512m",
                "spark.sql.shuffle.partitions": "2048",
                "spark.dynamicAllocation.maxExecutors": "6"
            },
            files={
                "application.properties"
            })

    def upload(self):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            match_cnt = self.job_msg.get_by_uuid(out['uuid'])
            print ("print match cnt for debug", match_cnt)

            if int(match_cnt['match_cnt']) == 0:
                continue

            # 导出dfs
            target_param = OptionParser.parse_target(out)

            target_dir = "%s/%s/%s" % (dataengine_env.dataengine_data_home, self.job_id, out['uuid'])
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)
            os.makedirs(target_param.name)

            status = shell.submit("hdfs dfs -get %s/*.* %s" % (out['hdfsOutput'], target_param.name))

            if status is not 0:
                shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
                raise DataEngineException("上传文件失败", "hdfs download failed")

            fast_dfs.tgz_upload(target_param)

    def inference_headers(self, file):
        if os.path.isdir(file):
            cmd = "head -n 1 %s/*" % file
        else:
            cmd = "head -n 1 %s" % file
        status, stdout = shell.submit_with_stdout(cmd)
        if status == 0:
            return stdout.strip()
        else:
            raise DataEngineException("获取文件表头失败")
