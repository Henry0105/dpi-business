# coding=utf-8
import json
import logging
import os
import shutil
import uuid

from module import tools
from module.const import main_class
from module.tools.cfg import prop_utils
from module.tools.dfs import fast_dfs
from module.tools.env import dataengine_env
from module.tools.opt import OptionParser
from module.tools.notify import MQ
from module.tools.utils import get_limit, get_limit_clause
from module.job.job import RpcJob
from module.job.error import DataEngineException
from module.tools.dir_context import DirContext
from module.tools import utils
from module.tools import shell

__author__ = 'zhangjt'


# 人群交并差
class CrowdSetOperation(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)

        job = {}
        job.update({'jobName': self.job['job_name'], 'jobId': self.job['job_id'], "day": self.job['day'],
                    "rpcHost": self.job['rpc_host'], "rpcPort": self.job['rpc_port']})

        for id_params, param in enumerate(self.job['param']):
            if job.get('params', None) == None:
                job.setdefault('params', [])

            if len(list(job['params'])) <= id_params:
                job['params'].append({})

            if job['params'][id_params].get('output', None) == None:
                job['params'][id_params].setdefault('output', {})
                job['params'][id_params]['output'].setdefault('opts', param['opts']['value'])
                job['params'][id_params]['output']['value'] = param['output']['value']
                if param['output'].get('uuid', None) == None:
                    job['params'][id_params]['output']['uuid'] = tools.utils.md5(param['output']['value'])
                else:
                    job['params'][id_params]['output']['uuid'] = param['output']['uuid']

            for id_inputs, input in enumerate(param['inputs']):

                if job['params'][id_params].get('inputs', None) == None:
                    job['params'][id_params].setdefault('inputs', [])

                if len(job['params'][id_params]['inputs']) <= id_inputs:
                    job['params'][id_params]['inputs'].append({})

                    # 如果input类型为dfs，写入hive,并生成指定uuid
                    if input['input_type'] == 'dfs':
                        file_uuid = str(uuid.uuid4())
                        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id
                        with DirContext(target_dir):
                            fast_dfs.download_with_decrypt_with_input(input, file_uuid)
                            self.persist_to_opt_table(file_uuid, file_uuid)

                        job['params'][id_params]['inputs'][id_inputs].setdefault('inputType', 'uuid')
                        job['params'][id_params]['inputs'][id_inputs].setdefault('uuid', file_uuid)
                    else:
                        job['params'][id_params]['inputs'][id_inputs].setdefault('inputType', input['input_type'])
                        job['params'][id_params]['inputs'][id_inputs].setdefault('uuid', input['value'])

                    job['params'][id_params]['inputs'][id_inputs].setdefault('name', input['name'])

        self.job = job
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]
        return job

    def receive(self, rpc_param):
        RpcJob.receive(self, rpc_param)
        event_code, rest = RpcJob.extract_code_and_rest(rpc_param)
        if event_code == "2":
            # {code}\u0001{uuid}\u0002{count}
            _, match_info = str.split(rest, u'\u0002')
            self.job_msg.put_into_details(json.loads(match_info))
        return True

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.CROWD_SET,
                "--driver-memory": "8g",
                "--executor-memory": "12g",
                "--executor-cores": "4",
            }
        )

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            # 如果是UUID,则不需要导出
            if "uuid" in output:
                uuid = output["uuid"]
            else:
                uuid = tools.utils.md5(output['value'])
            # output['limit'] = output['limit'] if 'limit' in output else 20000000
            output['limit'] = get_limit(output)
            target_param = OptionParser.parse_target(output)
            target_dir = os.path.dirname("{data_home}/{id}/".format(
                data_home=dataengine_env.dataengine_data_home,
                id=self.job_id + "/" + str(idx)
            ))
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)

            status, stdout = self.hive.submit_with_stdout(
                "SELECT count(1) FROM {target_table} where uuid='{uuid}'".format(
                    uuid=uuid,
                    target_table=self.target_hive_table)
            )

            target_param.extension = {
                "count_device": str.strip(stdout)
            }
            logging.info("count_device=>%s", stdout)

            limit_clause = get_limit_clause(output['limit'])

            status = self.hive.submit("""
                INSERT OVERWRITE LOCAL DIRECTORY '{dir}'  
                    SELECT data
                    FROM {target_table}
                    where uuid='{uuid}'
                    {limit_clause}
            """.format(uuid=uuid, dir=target_param.name, target_table=self.target_hive_table,
                       limit_clause=limit_clause))
            if status is not 0:
                raise DataEngineException("上传文件失败", "hdfs download failed")

            fast_dfs.tgz_upload(target_param)


# job_name=crowd_filter
class CrowdFilter(RpcJob):
    """
    人群包二次删选
    """

    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE
        self.mq = MQ()

    def receive(self, rpc_param):
        RpcJob.receive(self, rpc_param)
        event_code, rest = RpcJob.extract_code_and_rest(rpc_param)
        if event_code == "2":
            # {code}\u0001{uuid}\u0002{count}
            _, match_info = str.split(rest, u'\u0002')
            self.job_msg.put_into_details(json.loads(match_info))
        return True

    def _get_single_value(self, d, key, default_value=''):
        if key in d:
            return d[key].get('value', default_value)
        else:
            return default_value

    def _get_in_not_in_value(self, d, key):
        if key in d:
            in_value = ""
            notin_value = ""
            for e in d[key]:
                if 'opt' in e:
                    if 'in' == e['opt']:
                        in_value = e.get('value', '')
                    if 'notin' == e['opt']:
                        notin_value = e.get('value', '')
            return in_value, notin_value
        else:
            return "", ""

    def prepare(self):
        RpcJob.prepare(self)
        self.job['params'] = self.job.pop('param')
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id
        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                param["inputs"] = [param['input']]
                for input in param['inputs']:
                    # 由于种子包可以为空,则需要处理这种情况
                    if 'value' in input and len(input['value']) > 0:
                        if 'input_type' not in input or input['input_type'] != 'uuid':
                            file_uuid = str(uuid.uuid4())
                            fast_dfs.download_with_decrypt_with_input(input, file_uuid)
                            self.persist_to_opt_table(file_uuid, file_uuid)
                            input['uuid'] = file_uuid
                        else:
                            input['uuid'] = input['value']
                    else:
                        input['uuid'] = ""  # 设置一个空uuid,为了参数统一

        single_value_fields = ['agebin', 'car', 'edu', 'gender', 'income', 'kids', 'model_level', 'occupation',
                               'lat_lon_list']
        in_notin_fields = ['catelist', 'model', 'tag_list', 'cell_factory', 'country', 'province', 'city',
                           'permanent_country',
                           'permanent_province', 'permanent_city']
        for idx, param in enumerate(self.job['params']):
            input = param['inputs'][0]
            input['include'] = {}
            input['exclude'] = {}
            for field in single_value_fields:
                input['include'][field] = self._get_single_value(param, field)

            for field in in_notin_fields:
                in_value, notin_value = self._get_in_not_in_value(param, field)
                input['include'][field] = in_value
                input['exclude'][field] = notin_value

            if 'applist' in param:
                input['include']['applist'] = param['applist']['value']

            for idx, param in enumerate(self.job['params']):
                out = param['output']
                out['hdfs_output'] = target_dir + "/%s" % idx
                out["limit"] = get_limit(out)
                out["uuid"] = param['output']['uuid']

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name,
            job_id=self.job_id,
            props={
                "--class": main_class.CROWD_FILTER,
                "--driver-memory": "8g",
                "--executor-memory": "12g",
                "--executor-cores": "4",
            }
        )

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            uuid = output['uuid'] if 'uuid' in output else tools.utils.md5(output['value'])
            output_param = OptionParser.parse_target(output)
            output_param.extension = self.job_msg.get_by_uuid(output['uuid'])

            output_dir = os.path.dirname("{data_home}/{id}/".format(
                data_home=dataengine_env.dataengine_data_home,
                id=self.job_id + "/" + str(idx)
            ))
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)

            os.makedirs(output_dir)
            os.chdir(output_dir)

            limit_clause = get_limit_clause(output['limit'])

            status = self.hive.submit("""
                     INSERT OVERWRITE LOCAL DIRECTORY '{dir}'
                         SELECT data
                         FROM {target_table}
                          WHERE uuid='{uuid}'
                          {limit_clause}
                """.format(uuid=uuid, dir=output_param.name, target_table=self.target_hive_table,
                           limit_clause=limit_clause))
            if status is not 0:
                raise DataEngineException("上传文件失败", "hdfs download failed")

            mq_msg = output_param.extension.copy()

            output_param.extension.update({'count_device': output_param.extension['match_cnt']})
            fast_dfs.tgz_upload(output_param)

            logging.info("idmapping send mq with: " + json.dumps(mq_msg))
            # self.mq.send2(subject='dataengine_idmapping_msg', content=mq_msg)


# 人群交并差V2
class CrowdSetOperationV2(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id
        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                for input_idx, input in enumerate(param['inputs']):
                    if input['inputType'] == 'dfs':
                        file_uuid = str(uuid.uuid4())
                        input['uuid'] = file_uuid
                        fast_dfs.download_with_decrypt_with_input_v2(input, file_uuid)
                        self.persist_to_opt_table(file_uuid, file_uuid)
                    elif input['inputType'] == 'sql':
                        sql_uuid = str(uuid.uuid4())
                        sql_clause = utils.build_sql(input['value'])
                        input['uuid'] = sql_uuid
                        self.persist_to_opt_table_from_sql(sql_clause, sql_uuid)
                    else:
                        input['uuid'] = input['value']
                    input['name'] = input.get('name', 0)
                    input['inputType'] = 'uuid'
                    input['idType'] = input.get('idType', 4)
        for idx, param in enumerate(self.job['params']):
            out = param['output']
            out['opts'] = out.get('opts', None)
            out['limit'] = out.get('limit', -1)
            value = out.get('value', '')
            if value != '':
                out['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            else:
                out['hdfsOutput'] = ''
            # out['outputType'] = out.get('outputType', None)
            out['uuid'] = out.get('uuid', None)
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.CROWD_SET_V2,
                "--driver-memory": "8g",
                "--executor-memory": "12g",
                "--executor-cores": "4",
            }
        )

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


# 时间维度app状态筛选
class CrowdAppTimeFilter(RpcJob):
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
            job_name=self.job_name, job_id='{0}_{1}'.format(self.job_id, uuid.uuid4()),
            props={
                "--class": main_class.CrowdAppTimeFilter,
                "--driver-memory": "9g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
            },
            conf={
                "spark.driver.cores": "3",
                "spark.sql.shuffle.partitions": 1800,
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
