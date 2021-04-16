# coding=utf-8
import json
import logging
import os
import shutil
import uuid

from module.const import main_class
from module.tools.cfg import prop_utils
from module.tools.dfs import fast_dfs
from module.tools.env import dataengine_env
from module.tools.hive import Hive
from module.tools.opt import OptionParser
from module.tools.notify import MQ
from module.tools.utils import get_limit, get_limit_clause
from module.job.job import RpcJob
from module.job.error import DataEngineException
from module import tools
from module.tools.dir_context import DirContext
from module.tools import utils

__author__ = 'zhangjt'


class IdMapping(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.hive = Hive()
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

    def prepare(self):
        RpcJob.prepare(self)
        self.job['params'] = self.job.pop('param')
        target_dir = dataengine_env.dataengine_data_home + "/" + self.job_id

        with DirContext(target_dir):
            for idx, param in enumerate(self.job['params']):
                for input in param['inputs']:
                    if 'input_type' not in input or input['input_type'] != 'uuid':
                        file_uuid = str(uuid.uuid4())
                        fast_dfs.download_with_decrypt_with_input(input, file_uuid)
                        self.persist_to_opt_table(file_uuid, file_uuid)
                        input['uuid'] = file_uuid
                    else:
                        input['uuid'] = input['value']
                    input['encrypt'] = utils.build_encrypt_json(input)
                    input['id_type'] = input['id_type'].get('value', 4)

        for idx, param in enumerate(self.job['params']):
            param['output'] = param['outputs'][0]
            param.pop('outputs')
            out = param['output']
            out['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            out["limit"] = get_limit(param['output'])
            out['encrypt'] = utils.build_encrypt_json(out)
            out['id_types'] = out['id_types']['value']

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job['job_name'], job_id=self.job_id,
            props={
                "--class": main_class.ID_MAPPING,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf={
                "spark.kryoserializer.buffer.max": "512m"
            })

    def upload(self):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(out)
            target_param.extension = self.job_msg.get_by_uuid(out['uuid'])

            target_dir = "%s/%s/%s/%s" % (dataengine_env.dataengine_data_home, self.job_id, str(idx), out['uuid'])
            with DirContext(target_dir):
                limit = get_limit(out)
                limit_clause = get_limit_clause(limit)

                for out_id in out['id_types']:
                    status = self.hive.submit("""
                                INSERT OVERWRITE LOCAL DIRECTORY '{dir}'
                                SELECT data
                                    FROM {target_table}
                                    where uuid='{uuid}' and biz='3|{out_id}'
                                     {limit_clause}
                            """.format(uuid=out['uuid'],
                                       dir='%s/%s' % (target_param.name, out_id),
                                       target_table=self.target_hive_table,
                                       out_id=out_id,
                                       limit_clause=limit_clause))

                    if status is not 0:
                        raise DataEngineException("上传文件失败", "hdfs download failed")

                mq_msg = target_param.extension.copy()

                target_param.extension.update({'count_device': target_param.extension['match_cnt']})
                threshold = 20000000
                if (limit is None or limit > threshold) and \
                    len([v for _, v in mq_msg['out_cnt'].iteritems() if v > threshold]) > 0:
                    files = utils.get_split_files(target_param.name, threshold)
                    fast_dfs.tgz_upload_files(files, target_param)
                else:
                    fast_dfs.tgz_upload(target_param)
                logging.info("idmapping send mq with: " + json.dumps(mq_msg))
                # self.mq.send2(subject='dataengine_idmapping_msg', content=mq_msg)


class LocationMapping(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.hive = Hive()
        self.mq = MQ()
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

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
        self.job['params'] = self.job.pop('param')

        for idx, param in enumerate(self.job['params']):
            input = param['input']
            input['dailyOrMonthly'] = input['daily_monthly'].get('value', 1)
            input['beginDay'] = input['begin_day'].get('value', None)
            input['endDay'] = input['end_day'].get('value', None)
            input['areaType'] = input['area_type']['value']
            if input['areaType'] == 1: # 多边形
                input['latLonList'] = input['lat_lon_list']['value']
            else: # 区域
                input['areaCode'] = input['area_code'].get('value', None)
                input['cityCode'] = input['city_code'].get('value', None)

            param['inputs'] = [input]
            param['output']['hdfs_output'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            param['output']['limit'] = get_limit(param["output"])
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.LOCATION_DEVICE_MAPPING,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5",
                "--conf": "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        # for idx, param in enumerate(job['param']):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(out)
            target_param.extension = self.job_msg.get_by_uuid(out['uuid'])
            target_dir = os.path.dirname("%s/%s/%s/" % (dataengine_env.dataengine_data_home, self.job_id, str(idx)))
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)

            limit = get_limit(out)
            limit_clause = get_limit_clause(limit)

            status = self.hive.submit("""
                        INSERT OVERWRITE LOCAL DIRECTORY '{dir}'  
                            SELECT data
                            FROM {target_table}
                            where uuid='{uuid}' and biz='12|4'
                            {limit_clause}
                    """.format(uuid=out['uuid'],
                               dir='%s/%s' % (target_param.name, 4),
                               target_table=self.target_hive_table,
                               limit_clause=limit_clause))

            if status is not 0:
                raise DataEngineException("上传文件失败", "hdfs download failed")

            mq_msg = target_param.extension.copy()

            target_param.extension.update({'count_device': target_param.extension['match_cnt']})
            fast_dfs.tgz_upload(target_param)

            logging.info("idmapping send mq with: " + json.dumps(mq_msg))
            # self.mq.send2(subject='dataengine_idmapping_msg', content=mq_msg)


# 使用新版参数的地理围栏过滤
class LocationMappingV2(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.hive = Hive()
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

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

        for idx, param in enumerate(self.job['params']):
            param['output']['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            param['output']['limit'] = get_limit(param["output"])
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.LOCATION_DEVICE_MAPPING,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5",
                "--conf": "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        # for idx, param in enumerate(job['param']):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(out)
            target_param.extension = self.job_msg.get_by_uuid(out['uuid'])
            target_dir = os.path.dirname("%s/%s/%s/" % (dataengine_env.dataengine_data_home, self.job_id, str(idx)))
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)

            limit = get_limit(out)
            limit_clause = get_limit_clause(limit)

            status = self.hive.submit("""
                        INSERT OVERWRITE LOCAL DIRECTORY '{dir}'  
                            SELECT data
                            FROM {target_table}
                            where uuid='{uuid}' and biz='12|4'
                            {limit_clause}
                    """.format(uuid=out['uuid'],
                               dir='%s/%s' % (target_param.name, 4),
                               target_table=self.target_hive_table,
                               limit_clause=limit_clause))

            if status is not 0:
                raise DataEngineException("上传文件失败", "hdfs download failed")

            mq_msg = target_param.extension.copy()

            target_param.extension.update({'count_device': target_param.extension['match_cnt']})
            fast_dfs.tgz_upload(target_param)

            logging.info("location mapping send info with: " + json.dumps(mq_msg))
