# coding=utf-8
import json
import os
import shutil
import uuid

from module.const import main_class
from module.tools.cfg import prop_utils
from module.tools.dfs import fast_dfs
from module.tools.env import dataengine_env
from module.tools.opt import OptionParser
from module.job.job import RpcJob
from module.job.error import DataEngineException
from module.tools.dir_context import DirContext
from module.tools import utils
from module.tools import shell


# 多维过滤
class MultidimensionalFilter(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id
        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                for input_idx, input in enumerate(param['inputs']):
                    inputType_list = ['uuid', 'sql', 'empty']
                    if 'inputType' not in input or (input['inputType'] not in inputType_list):
                        input['inputType'] = 'dfs'
                        file_uuid = str(uuid.uuid4())
                        fast_dfs.download_with_decrypt_with_input_v2(input, file_uuid)
                        self.persist_to_opt_table(file_uuid, file_uuid)
                        input['uuid'] = file_uuid
                    if input['inputType'] == 'uuid':
                        input['uuid'] = input['value']
                    elif input['inputType'] == 'sql':
                        # 输入格式为sql的时候, input['uuid']为sql语句,需要进行转义
                        input['uuid'] = input['value'].replace('\'', '@')
                        input['uuid'] = input['uuid'].replace('\"', '@')
                        input['value'] = None
                        input['header'] = 1
                        input['idx'] = input.get('idx', 1)
                        input['sep'] = input.get('sep', ',')
                    elif input['inputType'] == 'empty':
                        input['value'] = None
                        input['uuid'] = None

        for idx, param in enumerate(self.job['params']):
            out = param['output']
            out['limit'] = out.get('limit', -1)
            value = out.get('value', '')
            if value != '':
                out['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            else:
                out['hdfsOutput'] = ''
            out['uuid'] = out.get('uuid', None)
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.MULTIDIMENSIONAL_FILTER,
                "--driver-memory": "8g",
                "--executor-memory": "12g",
                "--executor-cores": "4",
            },
            conf={
                "spark.kryoserializer.buffer.max": "512m"
            },
            files={
                "application.properties"
            })

    def upload(self):
        RpcJob.prepare(self)
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(out)
            target_dir = "%s/%s/%s" % (dataengine_env.dataengine_data_home, self.job_id, out['uuid'])
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)
            os.makedirs(target_param.name)

            status = shell.submit("hdfs dfs -get %s/*.csv %s" % (out['hdfsOutput'], target_param.name))

            if status is not 0:
                shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
                raise DataEngineException("上传文件失败", "hdfs download failed")

            fast_dfs.tgz_upload(target_param)

    def receive(self, rpc_param):
        RpcJob.receive(self, rpc_param)
        event_code, rest = RpcJob.extract_code_and_rest(rpc_param)
        if event_code == "2":
            # {code}\u0001{uuid}\u0002{count}
            _, match_info = str.split(rest, u'\u0002')
            self.job_msg.put_into_details(json.loads(match_info))
        return True

    def persist_to_opt_table_from_sql(self, sql_clause, uuid):
        self.hive.submit("""
            insert overwrite table {opt_table}
                partition(created_day='{day}', biz='from_sql', uuid='{uuid}')
            select device from (
                {sql_clause}
            ) tmp;
        """.format(opt_table=prop_utils.HIVE_TABLE_DATA_OPT_CACHE,
                   day=self.job['day'], uuid=uuid, sql_clause=sql_clause))