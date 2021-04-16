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


class ProfileCalScore(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        for idx, param in enumerate(self.job['param']):
            if 'param_id' not in param or param["param_id"] is None:
                param["param_id"] = str(uuid.uuid4())
            param['output']['limit'] = get_limit(param['output'])
            param['output']['day'] = param['output']['day'] if 'day' in param['output'] else utils.current_day()
            param['output']['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx

        job = {}
        job.update({'jobName': self.job['job_name'], 'jobId': self.job['job_id'], "day": self.job['day'],
                    "rpcHost": self.job['rpc_host'], "rpcPort": self.job['rpc_port']})
        for id_params, param in enumerate(self.job['param']):
            if job.get('params', None) == None:
                job['params'] =[]
            if len(list(job['params'])) <= id_params:
                job['params'].append({})
            job_param = job['params'][id_params]
            job_param['output'] = param['output']
            job_param_output = job_param['output']
            if param['output'].get('uuid', None) == None:
                job_param_output['uuid'] = tools.utils.md5(param['output']['value'])
            else:
                job_param_output['uuid'] = param['output']['uuid']['value']
            job_param['inputs'] = []
            job_param_inputs = job['params'][id_params]['inputs']
            job_param_inputs.append({})
            job_inputs_size = len(job_param_inputs)

            input = param['input']
            if 'input_type' in input and input['input_type'] != 'uuid':
                file_uuid = str(uuid.uuid4())
                fast_dfs.download_with_decrypt_with_input(input, file_uuid)
                self.persist_to_opt_table(file_uuid, file_uuid)
                job_param_inputs[job_inputs_size-1]['uuid'] = file_uuid
            else:
                param_input_value = input['value']
                job_param_inputs[job_inputs_size-1]['uuid'] = param_input_value

        self.job = job
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]
        for idx, output in self.outputs:
            output['day'] = job['day']

        return job

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.PROFILE_CAL_SCORE,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf={
                "spark.driver.maxResultSize": "3G",
                "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties"
            },
            files={
                "application.properties"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(output)
            target_dir = os.path.dirname("%s/%s/%s/" % (dataengine_env.dataengine_data_home, self.job_id, str(idx)))
            query = build_profile_query(dir_name=target_param.name,
                                        table_name=prop_utils.HIVE_TALBE_MOBEYE_O2O_BASE_SCORE_DAILY,
                                        where_clause="userid='{uuid}'".format(uuid=output['uuid']),
                                        limit=output['limit'],
                                        plain_fields=['type', 'sub_type', 'percent'])
            fast_dfs.upload_from_hive_query(output, target_dir, query)


class ProfileCalAppInfo(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        for idx, param in enumerate(self.job['param']):
            if 'param_id' not in param or param["param_id"] is None:
                param["param_id"] = str(uuid.uuid4())
            param['output']['limit'] = get_limit(param['output'])
            param['output']['day'] = param['output']['day'] if 'day' in param['output'] else utils.current_day()
            param['output']['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx

        job = {}
        job.update({'jobName': self.job['job_name'], 'jobId': self.job['job_id'], "day": self.job['day'],
                    "rpcHost": self.job['rpc_host'], "rpcPort": self.job['rpc_port']})
        for id_params, param in enumerate(self.job['param']):
            if job.get('params', None) == None:
                job['params'] = []
            if len(list(job['params'])) <= id_params:
                job['params'].append({})
            job_param = job['params'][id_params]
            job_param['output'] = param['output']
            job_param_output = job_param['output']
            if param['output'].get('uuid', None) == None:
                job_param_output['uuid'] = tools.utils.md5(param['output']['value'])
            else:
                job_param_output['uuid'] = param['output']['uuid']['value']
            job_param['output'] = job_param_output
            job_param['inputs'] = []
            job_param_inputs = job['params'][id_params]['inputs']
            job_param_inputs.append({})
            job_inputs_size = len(job_param_inputs)

            input = param['input']
            if 'input_type' in input and input['input_type'] != 'uuid':
                file_uuid = str(uuid.uuid4())
                fast_dfs.download_with_decrypt_with_input(input, file_uuid)
                self.persist_to_opt_table(file_uuid, file_uuid)
                job_param_inputs[job_inputs_size-1]['uuid'] = file_uuid
            else:
                param_input_value = input['value']
                job_param_inputs[job_inputs_size-1]['uuid'] = param_input_value

        self.job = job
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]
        for idx, output in self.outputs:
            output['day'] = job['day']

        return job

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.PROFILE_CAL_APP_INFO,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf={
                "spark.driver.maxResultSize": "3G",
                "spark.network.timeout": "300s",
                "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties"
            },
            files={
                "application.properties"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(output)
            target_dir = os.path.dirname("%s/%s/%s/" % (dataengine_env.dataengine_data_home, self.job_id, str(idx)))
            order = output.get('order', None)
            if order is not None:
                order_condition = \
                    "order by %s %s" %(output['order']['field'], 'asc' if output['order']['asc'] == 1 else 'desc')
            else:
                order_condition = ""
            query = build_profile_query(dir_name=target_param.name,
                                        table_name=prop_utils.HIVE_TABLE_APPINFO_DAILY,
                                        where_clause="userid='{uuid}' {order_condition}".format(uuid=output['uuid'],
                                        order_condition=order_condition),
                                        limit=output['limit'],
                                        plain_fields=['icon', 'name', 'cate_name', 'radio', 'index', 'apppkg',
                                         'cate_id', 'cate_l1'])
            fast_dfs.upload_from_hive_query(output, target_dir, query)


class ProfileCalAppInfoV2(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        for idx, param in enumerate(self.job['params']):
            param['output']['limit'] = get_limit(param['output'])
            param['output']['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            for input_idx, input in enumerate(param['inputs']):
                if 'inputType' not in input or input['inputType'] != 'uuid':
                    file_uuid = str(uuid.uuid4())
                    fast_dfs.download_with_decrypt_with_input_v2(input, file_uuid)
                    self.persist_to_opt_table(file_uuid, file_uuid)
                    input['uuid'] = file_uuid
                else:
                    input['uuid'] = input['value']

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.PROFILE_CAL_APP_INFO_V2,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf={
                "spark.driver.maxResultSize": "3G",
                "spark.network.timeout": "300s",
                "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties"
            },
            files={
                "application.properties"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            # 导出dfs
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


class ProfileCalFrequency(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        for idx, param in enumerate(self.job['param']):
            if 'param_id' not in param or param["param_id"] is None:
                param["param_id"] = str(uuid.uuid4())
#            param['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            param['output']['limit'] = get_limit(param['output'])
            param['output']['day'] = param['output']['day'] if 'day' in param['output'] else utils.current_day()
            param['output']['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx

        job = {}
        job.update({'jobName': self.job['job_name'], 'jobId': self.job['job_id'], "day": self.job['day'],
                    "rpcHost":self.job['rpc_host'],"rpcPort":self.job['rpc_port']})
        for id_params, param in enumerate(self.job['param']):
            if job.get('params', None) == None:
                job['params'] =[]
            if len(list(job['params'])) <= id_params:
                job['params'].append({})
            job_param = job['params'][id_params]
            job_param['output'] = param['output']
            job_param_output = job_param['output']
            if param['output'].get('uuid', None) == None:
                job_param_output['uuid'] = tools.utils.md5(param['output']['value'])
            else:
                job_param_output['uuid'] = param['output']['uuid']['value']
            job_param['inputs'] = []
            job_param_inputs = job['params'][id_params]['inputs']
            job_param_inputs.append({})
            job_inputs_size = len(job_param_inputs)

            input = param['input']
            if 'input_type' in input and input['input_type'] != 'uuid':
                file_uuid = str(uuid.uuid4())
                fast_dfs.download_with_decrypt_with_input(input, file_uuid)
                self.persist_to_opt_table(file_uuid, file_uuid)
                job_param_inputs[job_inputs_size-1]['uuid'] = file_uuid
            else:
                param_input_value = input['value']
                job_param_inputs[job_inputs_size-1]['uuid'] = param_input_value

        self.job = job
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]
        for idx, output in self.outputs:
            output['day'] = job['day']

        return job


    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.PROFILE_CAL_FREQUENCY,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf={
                "spark.driver.maxResultSize": "3G",
                "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties"
            },
            files={
                "application.properties"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(output)
            target_dir = os.path.dirname("%s/%s/%s/" % (dataengine_env.dataengine_data_home, self.job_id, str(idx)))
            q1 = build_profile_query(dir_name="%s/freq" % target_param.name,
                                     table_name=prop_utils.HIVE_TABLE_MOBEYE_O2O_LBS_FREQUENCY_DAILY,
                                        where_clause="userid='{uuid}'".format(uuid=output['uuid']),
                                        limit=output['limit'],
                                        plain_fields=['device', 'gender', 'agebin', 'segment', 'edu', 'kids', 'income',
                                         'occupation', 'house', 'repayment', 'car', 'married'],
                                     struct_fields={
                                         'frequency': ['lat', 'lon', 'province', 'city', 'area'],
                                         'geohash_center5': ['lat', 'lon'],
                                         'geohash_center6': ['lat', 'lon'],
                                         'geohash_center7': ['lat', 'lon'],
                                         'geohash_center8': ['lat', 'lon'],
                                         'type_1': ['lat', 'lon', 'name'],
                                         'type_2': ['lat', 'lon', 'name'],
                                         'type_3': ['lat', 'lon', 'name'],
                                         'type_4': ['lat', 'lon', 'name'],
                                         'type_5': ['lat', 'lon', 'name'],
                                         'type_6': ['lat', 'lon', 'name'],
                                         'type_7': ['lat', 'lon', 'name'],
                                         'type_8': ['lat', 'lon', 'name'],
                                         'type_9': ['lat', 'lon', 'name'],
                                         'type_10': ['lat', 'lon', 'name']
                                      })

            q2 = build_profile_query(dir_name="%s/hw" % target_param.name,
                                     table_name=prop_utils.HIVE_TABLE_MOBEYE_O2O_LBS_HOMEANDWORK_DAILY,
                                     where_clause="userid='{uuid}'".format(uuid=output['uuid']),
                                     limit=output['limit'],
                                     plain_fields=['device', 'gender', 'agebin', 'segment', 'edu', 'kids', 'income',
                                      'occupation', 'house', 'repayment', 'car', 'married'],
                                     struct_fields={
                                         'home': ['lat', 'lon', 'province', 'city', 'area'],
                                         'work': ['lat', 'lon', 'province', 'city', 'area'],
                                         'home_type': ['lat', 'lon', 'name'],
                                         'work_type': ['lat', 'lon', 'name'],
                                         'home_geohash_center5': ['lat', 'lon'],
                                         'home_geohash_center6': ['lat', 'lon'],
                                         'home_geohash_center7': ['lat', 'lon'],
                                         'home_geohash_center8': ['lat', 'lon'],
                                         'work_geohash_center5': ['lat', 'lon'],
                                         'work_geohash_center6': ['lat', 'lon'],
                                         'work_geohash_center7': ['lat', 'lon'],
                                         'work_geohash_center8': ['lat', 'lon']
                                     })
            query = ';'.join([q1, q2])
            fast_dfs.upload_from_hive_query(output, target_dir, query)


class ProfileCalSourceAndFlow(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        for idx, param in enumerate(self.job['param']):
            if 'param_id' not in param or param["param_id"] is None:
                param["param_id"] = str(uuid.uuid4())
#            param['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            param['output']['limit'] = get_limit(param['output'])
            param['output']['day'] = param['output']['day'] if 'day' in param['output'] else utils.current_day()
            param['output']['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx

        job = {}
        job.update({'jobName': self.job['job_name'], 'jobId': self.job['job_id'], "day": self.job['day'],
                    "rpcHost":self.job['rpc_host'],"rpcPort":self.job['rpc_port']})
        for id_params, param in enumerate(self.job['param']):
            if job.get('params', None) == None:
                job['params'] =[]
            if len(list(job['params'])) <= id_params:
                job['params'].append({})
            job_param = job['params'][id_params]
            job_param['output'] = param['output']
            job_param_output = job_param['output']
            if param['output'].get('uuid', None) == None:
                job_param_output['uuid'] = tools.utils.md5(param['output']['value'])
            else:
                job_param_output['uuid'] = param['output']['uuid']['value']
            job_param['inputs'] = []
            job_param_inputs = job['params'][id_params]['inputs']
            job_param_inputs.append({})
            job_inputs_size = len(job_param_inputs)

            input = param['input']
            if 'input_type' in input and input['input_type'] != 'uuid':
                file_uuid = str(uuid.uuid4())
                fast_dfs.download_with_decrypt_with_input(input, file_uuid)
                self.persist_to_opt_table(file_uuid, file_uuid)
                job_param_inputs[job_inputs_size-1]['uuid'] = file_uuid
            else:
                param_input_value = input['value']
                job_param_inputs[job_inputs_size-1]['uuid'] = param_input_value

        self.job = job
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]
        for idx, output in self.outputs:
            output['day'] = job['day']

        return job

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.PROFILE_CAL_SOURCE_FLOW,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf={
                "spark.driver.maxResultSize": "3G",
                "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:log4j.properties"
            },
            files={
                "application.properties"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        for idx, output in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(output)
            target_dir = os.path.dirname("%s/%s/%s/" % (dataengine_env.dataengine_data_home, self.job_id, str(idx)))
            query = build_profile_query(dir_name=target_param.name,
                                        table_name=prop_utils.HIVE_TABLE_MOBEYE_O2O_LBS_SOUREANDFLOW_DAILY,
                                        where_clause="userid='{uuid}'".format(uuid=output['uuid']),
                                        limit=output['limit'],
                                        plain_fields=['device', 'gender', 'agebin', 'segment', 'edu', 'kids', 'income',
                                         'occupation', 'house', 'repayment', 'car', 'married', 'hour'],
                                        struct_fields={
                                            'source': ['lat', 'lon', 'province', 'city', 'area'],
                                            'flow': ['lat', 'lon', 'province', 'city', 'area'],
                                            's_geohash_center5': ['lat', 'lon'],
                                            's_geohash_center6': ['lat', 'lon'],
                                            's_geohash_center7': ['lat', 'lon'],
                                            's_geohash_center8': ['lat', 'lon'],
                                            'f_geohash_center5': ['lat', 'lon'],
                                            'f_geohash_center6': ['lat', 'lon'],
                                            'f_geohash_center7': ['lat', 'lon'],
                                            'f_geohash_center8': ['lat', 'lon'],
                                            'f_type_1': ['lat', 'lon', 'name'],
                                            'f_type_2': ['lat', 'lon', 'name'],
                                            'f_type_3': ['lat', 'lon', 'name'],
                                            's_type_1': ['lat', 'lon', 'name'],
                                            's_type_2': ['lat', 'lon', 'name'],
                                            's_type_3': ['lat', 'lon', 'name']
                                         })
            fast_dfs.upload_from_hive_query(output, target_dir, query)


# 查询画像相关表,并对struct类型进行拼接
def build_profile_query(dir_name, table_name, where_clause, order_clause=None, limit=None, plain_fields=[], struct_fields={}):
    all_fields = ','.join(plain_fields + [f for f in struct_fields])
    plain_fields_query = ["""cast({f} as string) as {f}""".format(f=f) for f in plain_fields]
    struct_fields_query = ["""concat_ws(',', {sf}) as {f}"""
                               .format(f=f, sf=','.join(["""concat('{k}=', {f}.{k})""".format(f=f, k=k) for k in keys]))
                           for f, keys in struct_fields.items()]
    fields_query = ','.join(plain_fields_query + struct_fields_query)
    return """
        INSERT OVERWRITE LOCAL DIRECTORY '{dir}'
        ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\u0001'  null defined as ''
        SELECT {all_fields}
        from (
          select {fields_query}
          from {table_name}
          where {where_clause}
        ) as tmp
        {order_clause}
        {limit_clause}
    """.format(where_clause=where_clause, order_clause="" if order_clause is None else order_clause,
               dir=dir_name, table_name=table_name, all_fields=all_fields,
               fields_query=fields_query,
               limit_clause="" if limit is None else "limit %s" % str(limit))


class ProfileCalBatchMonomer(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.mq = MQ()
        self.target_hive_table = prop_utils.HIVE_TABLE_DEVICE_SINGLE_PROFILE_INFO

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
                    if 'inputType' not in input or (input['inputType'] != 'uuid' and input['inputType'] != 'sql'):
                        input['inputType'] = 'dfs'
                        file_uuid = str(uuid.uuid4())
                        fast_dfs.download_with_decrypt_with_input_v2(input, file_uuid)
                        self.persist_to_opt_table(file_uuid, file_uuid)
                        input['uuid'] = file_uuid
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
            out['keepSeed'] = out.get('keepSeed', 1)

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.PROFILE_CAL_MONOMER,
                "--driver-memory": "10g",
                "--executor-memory": "8g",
                "--executor-cores": "4",
                "--num-executors": "5"
            },
            conf={
                "spark.driver.maxResultSize": "3G",
                "spark.kryoserializer.buffer.max": "512m",
                "spark.sql.shuffle.partitions": "2048"
            },
            files={
                "application.properties"
            },
            hdfs_jars={
                 'hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar',
                 'hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar'
            })

    def upload(self):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            match_cnt = self.job_msg.get_by_uuid(out['uuid'])
            print ("print match cnt for debug", match_cnt)
            # self.mq.send2(subject='dataengine_profile_batch_mono_msg', content=match_cnt)

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

            status = shell.submit("hdfs dfs -get %s/*/*.csv %s" % (out['hdfsOutput'], target_param.name))

            if status is not 0:
                shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
                raise DataEngineException("上传文件失败", "hdfs download failed")

            fast_dfs.tgz_upload(target_param)


class ProfileCalGroup(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.mq = MQ()

    def receive(self, rpc_param):
        RpcJob.receive(self, rpc_param)
        event_code, rest = RpcJob.extract_code_and_rest(rpc_param)
        if event_code == "2":
            # {code}\u0001{uuid}\u0002{count}
            _, match_info = str.split(rest, u'\u0002')
            self.job_msg.put_into_details(json.loads(match_info))
        return True

    def prepare(self):
        RpcJob.prepare(self)
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id

        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                for input_idx, input in enumerate(param['inputs']):
                    if 'inputType' not in input or input['inputType'] != 'uuid':
                        file_uuid = str(uuid.uuid4())
                        fast_dfs.download_with_decrypt_with_input_v2(input, file_uuid)
                        self.persist_to_opt_table(file_uuid, file_uuid)
                        input['uuid'] = file_uuid
                    else:
                        input['uuid'] = input['value']
                    # self.job['params'][idx]['inputs'].append(input)

        for idx, param in enumerate(self.job['params']):
            out = param['output']
            out['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            out['display'] = out.get('display', 0)
            self.job['params'][idx]['output'] = out

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.PROFILE_CAL_GROUP,
                "--driver-memory": "8g",
                "--executor-memory": "8g",
                "--executor-cores": "4",
                "--num-executors": "5"
            },
            conf={
                "spark.kryoserializer.buffer.max": "512m",
                "spark.sql.shuffle.partitions": "8000",
                "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors
            },
            files={
                "application.properties"
            })

    def upload(self):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            # 导出dfs
            target_param = OptionParser.parse_target(out)

            target_dir = "%s/%s/%s" % (dataengine_env.dataengine_data_home, self.job_id, out['uuid'])
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)

            status = shell.submit("hdfs dfs -get %s/*.csv %s" % (out['hdfsOutput'], target_param.name))

            if status is not 0:
                shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
                raise DataEngineException("上传文件失败", "hdfs download failed")

            fast_dfs.tgz_upload(target_param)
