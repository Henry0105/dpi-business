# coding=utf-8
import json
import os
import shutil
import uuid
import re

from module.const import main_class
from module.job.error import DataEngineException
from module.tools.cfg import prop_utils
from module.tools.dfs import fast_dfs
from module.tools.env import dataengine_env
from module.tools.opt import OptionParser
from module.tools import shell
from module.tools.dir_context import DirContext
from module.tools import utils
from module.job.job import RpcJob


class DataCleaning(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE_NEW

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
                for input in param['inputs']:
                    input['header'] = input.get('header', 0)
                    if 'inputType' in input and input['inputType'] == "sql":
                        """
                            idx不需处理, header 置为1, headers从SQL解析, sep置为四个空格
                        """
                        sql_uuid = str(uuid.uuid4())
                        (headers, clause) = self.get_headers_clause(input['value'])
                        input['headers'] = ['in_%s' % h.strip() for h in headers.split(',')]
                        input['sep'] = "".join([' '] * 4)
                        sql_clause = self.build_sql(input['value'])
                        self.persist_to_opt_table_from_sql(sql_clause, sql_uuid)
                        input['uuid'] = sql_uuid
                        input['header'] = 1
                        if 'idx' not in input:
                            input['idx'] = 1
                    elif 'inputType' not in input or input['inputType'] != 'uuid':
                        file_uuid = str(uuid.uuid4())
                        fast_dfs.download_with_decrypt_with_input_v2(input, file_uuid)
                        self.persist_to_opt_table(file_uuid, file_uuid)
                        input['uuid'] = file_uuid
                        # 对传入的header加前缀,以防重复
                        headers_str = self.inference_headers(file_uuid)
                        if input['header'] > 0:
                            if 'sep' in input:
                                input['headers'] = ['in_%s' % h for h in headers_str.split(input['sep'])]
                            else:  # 只有1列数据
                                input['sep'] = ','
                                input['idx'] = 1
                                input['headers'] = ['in_%s' % headers_str]
                        elif 'sep' in input:  # 没有header,但是有分割符,此时手动添加headers
                            input['header'] = 1
                            input['headers'] = ['in_%d' % i for i in range(len(headers_str.split(input['sep'])))]
                        else:
                            pass
                    else:
                        input['uuid'] = input['value']

        for idx, param in enumerate(self.job['params']):
            out = param['output']
            value = out.get('value', '')
            if value != '':
                out['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
            else:
                out['hdfsOutput'] = ''
            out["limit"] = out.get('limit', 20000000)

        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.DATA_CLEANING,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "3",
                "--num-executors": "5"
            },
            conf={
                "spark.kryoserializer.buffer.max": "512m"
            },
            jars=["udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar"])

    def upload(self):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(out)
            target_param.extension = target_param.extension = self.job_msg.get_by_uuid(out['uuid'])

            target_dir = "%s/%s/%s/%s" % (dataengine_env.dataengine_data_home, self.job_id, str(idx), out['uuid'])
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir)
            os.chdir(target_dir)

            status = shell.submit("hdfs dfs -get %s/*.csv %s" % (out['hdfsOutput'], target_param.name))

            if status is not 0:
                shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
                raise DataEngineException("上传文件失败", "hdfs download failed")

            target_param.extension.update({'count_device': target_param.extension['match_cnt']})
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

    def get_headers_clause(self, raw_sql_clause):
        result = re.search('select(.*)from(.*)', raw_sql_clause, re.IGNORECASE)
        print ("headers is ", result.group(1))
        print ("table name filter and other is ", result.group(2))
        headers = result.group(1)
        clause = result.group(2)
        return headers, clause

    def build_sql(self, raw_sql_clause):
        (headers, clause) = self.get_headers_clause(raw_sql_clause)
        # 列内2个空格  列间4个空格
        tmp_clause = "( select {cols} from {clause} ) tmp_clause ".format(
            cols=",".join([h.strip() for h in headers.split(',')]), clause=clause)
        cols = "CONCAT_WS('{sep}',{colnames}) as device".format(
            sep="".join([' '] * 4),
            colnames=",".join(
                ["CONCAT_WS('{sep}',CAST({colname} AS string))".format(sep="".join([' '] * 2), colname=h.strip())
                 for h in headers.split(',')])
        )
        return "select {cols} from {clause}".format(cols=cols, clause=tmp_clause)
