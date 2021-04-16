# coding=utf-8
import json
import os
import shutil
import uuid

from module.const.main_class import CROWD_PORTRAIT_ADJUSTER, CROWD_PORTRAIT_ESTIMATION, CROWD_PORTRAIT_CALCULATION
from module.tools import utils
from module.tools.cfg import prop_utils
from module.tools.dfs import fast_dfs
from module.tools.env import dataengine_env
from module.tools.opt import OptionParser
from module import tools
from module.job.job import RpcJob
from module.tools.dir_context import DirContext

__author__ = 'zhangjt'


class CustomCrowd(RpcJob):
    """
    自定义群体画像标签
    """

    def __init__(self, job, target_table):
        RpcJob.__init__(self, job)
        self.target_table = target_table

    def prepare(self):
        RpcJob.prepare(self)
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
                    input['tagList'] = param['tagList'].get('value', None)

        for idx, param in enumerate(self.job['params']):
            out = param['output']
            out['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            out["limit"] = utils.get_limit(param['output'])

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            uuid = output['uuid']
            if not uuid.strip():
                uuid = utils.md5(output['value'])
            target_param = OptionParser.parse_target(output)
            target_dir = os.path.dirname("{data_home}/{id}/".format(
                data_home=dataengine_env.dataengine_data_home,
                id=self.job_id + "/" + str(idx)
            ))
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)

            # TODO 优化 需要从hdfs直接获取
            self.hive.submit("""
                    INSERT OVERWRITE LOCAL DIRECTORY '{dir}'
                        SELECT *
                        FROM {target_table}
                        where uuid='{uuid}'
                """.format(uuid=uuid, dir=target_param.name, target_table=self.target_table))

            fast_dfs.tgz_upload(target_param)


class CrowdCalculation(CustomCrowd):
    def __init__(self, job):
        CustomCrowd.__init__(self, job, prop_utils.HIVE_TABLE_CROWD_PORTRAIT_CALCULATION_SCORE)

    def run(self):
        self.spark2.submit(
            args="\'%s\'" % json.dumps(self.job, indent=1),
            job_name=self.job_name,
            job_id=self.job_id,
            props={
                "--class": CROWD_PORTRAIT_CALCULATION,
                "--driver-memory": "10g",
                "--executor-memory": "6g",
                "--executor-cores": "2"},
            conf={
                "spark.dynamicAllocation.maxExecutors": "60"
            },
            files=["crowd-portrait.json"],
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])


class CrowdEstimation(CustomCrowd):
    def __init__(self, job):
        CustomCrowd.__init__(self, job, prop_utils.HIVE_TABLE_CROWD_PORTRAIT_ESTIMATION_SCORE)

    def run(self):
        self.spark2.submit(
            args="\'%s\'" % json.dumps(self.job, indent=1),
            job_name=self.job_name,
            job_id=self.job_id,
            props={
                "--class": CROWD_PORTRAIT_ESTIMATION,
                "--driver-memory": "10g",
                "--executor-memory": "6g",
                "--executor-cores": "2"},
            conf={
                "spark.dynamicAllocation.maxExecutors": "60"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])


class CrowdAdjust(CustomCrowd):
    def __init__(self, job):
        CustomCrowd.__init__(self, job, prop_utils.HIVE_TABLE_CROWD_PORTRAIT_ADJUST_CALCULATION_SCORE)

    def run(self):
        self.spark2.submit(
            args="\'%s\'" % json.dumps(self.job, indent=1),
            job_name=self.job_name,
            job_id=self.job_id,
            props={
                "--class": CROWD_PORTRAIT_ADJUSTER,
                "--driver-memory": "2g",
                "--executor-memory": "2g",
                "--executor-cores": "2"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"]
        )
