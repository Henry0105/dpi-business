# coding=utf-8

import json
import os
import shutil
import uuid

from module.const import main_class
from module.tools import shell
from module.tools.check_cont import CheckCont, CheckType
from module.tools.dfs import fast_dfs
from module.tools.env import dataengine_env
from module.tools.notify import MQ
from module.tools.opt import OptionParser
from module.tools.dir_context import DirContext
from module.tools.utils import get_limit
from module.tools import utils
from module import tools
from module.job.job import RpcJob
from module.job.error import DataEngineException

__author__ = 'zhangjt'


class ProfileBatchBackTracker(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.mq = MQ()

    def receive(self, rpc_param):
        if rpc_param.startswith("1"):
            RpcJob.receive(self, rpc_param)
        else:
            p = json.loads(rpc_param)
            if p['code'] == 2:
                match_cnt_data = p['data']['match_info']
                arr = str.split(str(match_cnt_data), u'\u0001')
                # {code}\u0001{uuid}\u0002{count}
                t1 = str(arr[1])
                _, match_info = str.split(t1, u'\u0002')
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
        # self.job['params'] = self.job.pop('param')
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id

        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                # param['inputs'] = [param.pop('input')]
                for input in param['inputs']:
                    input['trackDayIndex'] = input.get('trackDayIndex', 2)
                    if 'inputType' not in input or (input['inputType'] != 'uuid' and input['inputType'] != 'sql'):
                        file_uuid = str(uuid.uuid4())
                        fast_dfs.download_with_decrypt_with_input_v2(input, file_uuid)
                        self.persist_to_opt_new_table(file_uuid, file_uuid)
                        input['uuid'] = file_uuid
                    elif input['inputType'] == 'uuid':
                        input['uuid'] = input['value']
                    elif input['inputType'] == 'sql':
                        # 输入格式为sql的时候, input['uuid']为sql语句,需要进行转义
                        input['uuid'] = input['value'].replace('\'', '@')
                        input['uuid'] = input['uuid'].replace('\"', '@')
                        input['value'] = None

        for idx, param in enumerate(self.job['params']):
            out = param['output']
            out['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            out["limit"] = get_limit(param['output'])

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.PROFILE_EXPORT_BT,
                "--driver-memory": "8g",
                "--executor-memory": "8g",
                "--executor-cores": "4",
                "--num-executors": "5"
            },
            conf={
                "spark.driver.maxResultSize": "3G",
                "spark.kryoserializer.buffer.max": "512m",
                "spark.sql.shuffle.partitions": "600",
                "spark.dynamicAllocation.maxExecutors": "130",
                "spark.scheduler.listenerbus.eventqueue.size": "100000"
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
            target_param.extension = match_cnt

            target_dir = "%s/%s/%s" % (dataengine_env.dataengine_data_home, self.job_id, out['uuid'])
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)
            os.makedirs(target_param.name)

            status = shell.submit("hdfs dfs -get %s/*/*.csv %s" % (out['hdfsOutput'], target_param.name))

            if status is not 0:
                shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
                raise DataEngineException("上传文件失败", "hdfs download failed")

            fast_dfs.tgz_upload(target_param)
