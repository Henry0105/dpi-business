# coding=utf-8
import json
import uuid
import os
import shutil

from module import tools
from module.tools.cfg import prop_utils
from module.tools.dfs import fast_dfs
from module.tools.env import dataengine_env
from module.tools.opt import OptionParser
from module.tools.utils import get_limit, get_limit_clause
from module.tools.dir_context import DirContext
from module.tools import utils
from module.job.job import RpcJob

__author__ = 'zhangjt'


class SelectionOptimization(RpcJob):
    """
    自定义群体画像标签
    """

    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE
        self.outputs = None

    def prepare(self):
        RpcJob.prepare(self)
        # todo 这个limit参数需要统一
        self.job['params'] = self.job.pop('param')
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id

        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                param['inputs'] = [param.pop('input')]
                for input in param['inputs']:
                    if 'input_type' not in input or input['input_type'] != 'uuid':
                        file_uuid = str(uuid.uuid4())
                        fast_dfs.download_with_decrypt_with_input(input, file_uuid)
                        self.persist_to_opt_table(file_uuid, file_uuid)
                        input['uuid'] = file_uuid
                    else:
                        input['uuid'] = input['value']
                    input['id_type'] = 4
                    input['option'] = param['option']
                    input['option']['option_type'] = param['option'].pop('type')
                    input['option']['option_value'] = param['option'].pop('value', '')

        for idx, param in enumerate(self.job['params']):
            out = param['output']
            out['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            out["limit"] = get_limit(param['limit'], "value")

        # for param in self.job['param']:
        #     if 'limit' in param and 'value' in param['limit']:
        #         param['output']['limit'] = param['limit']['value']
        #     else:
        #         param['output']['limit'] = 20000000

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args="\'%s\'" % json.dumps(self.job, indent=1),
            job_name=self.job_name,
            job_id=self.job_id,
            props={
                "--class": "com.mob.dataengine.engine.core.crowd.CrowdSelectionOptimization",
                "--driver-memory": "10g",
                "--executor-memory": "12g",
                "--executor-cores": "2",
                "--num-executors": "40"},
            conf={
                "spark.dynamicAllocation.maxExecutors": "60"
            },
            files=["hive_database_table.properties"])

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            # 如果是UUID,则不需要导出
            if "output_type" not in output or output['output_type'] is not "uuid":
                uuid = output['uuid']
                if not uuid.strip():
                    uuid = tools.utils.md5(output['value'])
                target_param = OptionParser.parse_target(output)
                target_dir = os.path.dirname("{data_home}/{id}/".format(
                    data_home=dataengine_env.dataengine_data_home,
                    id=self.job['job_id'] + "/" + str(idx)
                ))
                if os.path.exists(target_dir):
                    shutil.rmtree(target_dir)

                os.makedirs(target_dir)
                os.chdir(target_dir)

                limit = get_limit(output)
                limit_clause = get_limit_clause(limit)

                # TODO 优化 需要从hdfs直接获取
                self.hive.submit("""
                        INSERT OVERWRITE LOCAL DIRECTORY '{dir}'  
                            SELECT data
                            FROM {target_table}
                            where uuid='{uuid}'
                            {limit_clause}
                    """.format(uuid=uuid, dir=target_param.name, target_table=self.target_hive_table,
                               limit_clause=limit_clause))

                fast_dfs.tgz_upload(target_param)
