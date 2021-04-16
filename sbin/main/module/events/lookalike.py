# coding=utf-8
import json
import shutil
import os
import uuid

from module.const import main_class
from module.tools.cfg import prop_utils
from module.tools.shell import submit_with_stdout
from module.tools.opt import OptionParser
from module.tools.env import dataengine_env
from module.tools.dfs import fast_dfs
from module.tools.utils import get_limit
from module.job.job import RpcJob
from module.job.error import DataEngineException
from module import tools
from module.tools.dir_context import DirContext
from module.tools import utils

__author__ = 'zhangjt'


# todo 需要直接读hive rest
def last_partitions_day(table, field):
    (status, stdout) = submit_with_stdout("hive -e 'show partitions %s'" % table)
    if status is 0:
        pars = filter(lambda s: str.strip(s).startswith(field + "="), str.split(stdout))
        if len(pars) is 0:
            return ""
        par = pars[len(pars) - 1]
        return str.split(str.strip(par), "=")[1]
    else:
        raise Exception("execute %s => %s" % status)


class Normal(RpcJob):
    """
    @pactfidf_day:rp_dataengine.rp_mobeye_tfidf_pca 最后一个分区
    """

    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        for idx, param in enumerate(self.job['param']):
            output = param['output']
            output['uuid'] = output['uuid'] if 'uuid' in output else str(uuid.uuid4())
            output['limit'] = get_limit(output)

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['param'])]

        job = {}
        job.update({'jobName': self.job['job_name'], 'jobId': self.job['job_id'], "day": self.job['day'],
            "rpcHost":self.job['rpc_host'],"rpcPort":self.job['rpc_port']})
        for id_params, param in enumerate(self.job['param']):
            if job.get('params', None) == None:
                job['params'] =[]

            if len(list(job['params'])) <= id_params:
                job['params'].append({})

            job_param = job['params'][id_params]
            if job_param.get('output', None) == None:
                job_param['output'] = {}
                job_param_output = job_param['output']

                if param['output'].get('uuid', None) == None:
                    job_param_output['uuid'] = tools.utils.md5(param['output']['value'])
                else:
                    job_param_output['uuid'] = param['output']['uuid']
                job_param_output['hdfs_output'] = ""

            job_param['inputs'] = []
            job_param_inputs = job['params'][id_params]['inputs']
            job_param_inputs.append({})
            job_inputs_size = len(job_param_inputs)
            param_input_value = param['input']['value']
            if param['input'].get('input_type', None) != 'uuid':
                file_uuid = str(uuid.uuid4())
                job_param_inputs[job_inputs_size-1]['uuid'] = file_uuid
                target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id
                with DirContext(target_dir):
                    fast_dfs.download_with_decrypt_with_input(param['input'], file_uuid)
                    self.persist_to_opt_table(file_uuid, file_uuid)
            else:
                job_param_inputs[job_inputs_size-1]['uuid'] = param_input_value
            job_param_inputs[job_inputs_size-1]['idType'] = param['input']['id_type'].get('value', 4)

        self.job = job
        return job

    def run(self):
        """
        实现功能：提取设备的在装app列表，标签列表以及用户画像，得到当天设备标签列表和用户画像。
            
            实现逻辑：
            1.种子用户和device_id_mapping join获取设备号
            2.设备号和pca后的tfidf获取种子用户tfidf
            3.求出种子用户的中心点
            4.算出离中心点最近的设备
            输出结果：表名（{table}）scoring结果表
        """.format(table=prop_utils.HIVE_TABLE_DATA_OPT_CACHE)

        self.spark2.submit(
            "dataengine-core", "\'%s\'" % json.dumps(self.job, indent=1), self.job_name, self.job_id,
            props={
                "--class": main_class.LOOKALIKE_DISTANCE_CALCULATION,
                "--driver-memory": "10g",
                "--executor-memory": "12g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf = {
                "spark.sql.shuffle.partitions": "1000",
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(output)
            target_dir = os.path.dirname("%s/%s/%s/" % (dataengine_env.dataengine_data_home, self.job_id, str(idx)))
            order = output.get('order', None)
            if order is not None:
                order_condition = \
                    "order by split(data, '\u0001')[1] %s" %('asc' if output['order']['asc'] == 1 else 'desc')
            else:
                order_condition = ""
            fast_dfs.upload_from_hive_cache(output=output,
                                            where="uuid='%s' and biz='%s' %s" % (output['uuid'], '11|4', order_condition),
                                            data_dir='%s/%s' % (target_param.name, '4'),
                                            target_dir=target_dir)