# coding=utf-8
import csv
import json
import os
import shutil
import uuid
from module.tools.cfg import prop_utils
from module.job.job import RpcJob
from module.tools.opt import OptionParser
from module.tools.dfs import fast_dfs
from module.tools import utils
from module.tools import shell
from module.tools.env import dataengine_env
from module.job.error import DataEngineException


class EddUtils(RpcJob):
    keyListEN = ["FIELD_INDEX", "FIELD_NAME", "FIELD_TYPE", "BLANKS_NUM", "ENTRIES_NUM", "UNIQUE_NUM", "POP_STDEV",
                 "MEAN_OR_TOP1", "MIN_OR_TOP2", "P1_OR_TOP3", "P5_OR_TOP4", "P25_OR_TOP5", "P50_OR_BOT5", "P75_OR_BOT4",
                 "P95_OR_BOT3", "P99_OR_BOT2", "MAX_OR_BOT1"]

    keyListCN = [u"索引", u"列名", u"类型", u"非法值计数", u"合法值计数", u"去重值计数", u"总体标准差", u"平均值",
                 u"最小值", u"1分位数", u"5分位数", u"25分位数", u"50分位数", u"75分位数", u"95分位数", u"99分位数", u"最大值"]

    def __init__(self, job):

        RpcJob.__init__(self, job)
        self.target_hive_table = prop_utils.HIVE_TABLE_DATA_OPT_CACHE

    def prepare(self):
        RpcJob.prepare(self)
        for idx, param in enumerate(self.job['params']):
            param['output']['uuid'] = str(uuid.uuid4())
            param['output']['hdfsOutput'] = dataengine_env.dataengine_hdfs_data_home + "/" + self.job_id + "/%s" % idx
        self.outputs = [(idx, param['output']) for idx, param in enumerate(self.job['params'])]

    def run(self):
        spark_sh = """
            {mob_utils} --datachk --sql '{sqlText}' --hdfs_output_path {hdfsOutputPath} --err {errLevel}
            """.format(
            mob_utils=dataengine_env.mobutils_path,
            sqlText=self.job['params'][0]["inputs"][0]['value'],
            hdfsOutputPath=self.job['params'][0]['output']['hdfsOutput'],
            errLevel=self.job['params'][0]['inputs'][0]['err']
        )
        print "spark_shell:", spark_sh
        status = shell.submit(spark_sh)
        if status is not 0:
            raise Exception("shell failed %s" % status)
        return status

    def upload(self):
        for idx, out in utils.filter_outputs_by_value(self.outputs):
            target_param = OptionParser.parse_target(out)
            target_dir = "%s/%s/%s" % (dataengine_env.dataengine_data_home, self.job_id, out['uuid'])
            print "target_dir is :", target_dir
            print "target_param.name is:", target_param.name
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)
            os.makedirs(target_dir)
            os.chdir(target_dir)
            os.makedirs(target_param.name)
            status = shell.submit("hdfs dfs -get %s/*.json %s" % (out['hdfsOutput'], target_param.name))
            if status is not 0:
                shell.submit("hdfs dfs -rm -r %s/%s" % (dataengine_env.dataengine_hdfs_data_home, self.job_id))
                raise DataEngineException("下载文件失败", "hdfs download failed")
            json_to_csv(target_param.name, target_param.name, EddUtils.keyListCN, EddUtils.keyListEN)
            fast_dfs.tgz_upload(target_param)


def json_to_csv(input_path, output_path, key_list_cn, key_list_en):
    [tran(f, input_path, output_path, key_list_cn, key_list_en) for f in os.listdir(input_path) if f.endswith(".json")]


def tran(f, input_path, output_path, key_list_cn, key_list_en):
    csv_file = f.replace('.json', '.csv')
    with open('%s/%s' % (input_path, f), 'r') as new_json_file, open('%s/%s' % (output_path, csv_file), 'wb') \
            as csv_file:
        writer = csv.writer(csv_file, delimiter='\t')
        writer.writerow([s.encode('utf-8') for s in key_list_cn])

        for lineJson in new_json_file:
            dic = json.loads(lineJson, encoding='utf-8')
            values_list = []
            for key in key_list_en:
                values_list.append(dic[key])

            # writer.writerow([s.encode('utf-8') if not isinstance(s, int) else s for s in values_list])
            # writer.writerow([s.encode('utf-8') if isinstance(s, str) else s for s in values_list])--废弃，还有其他的特殊字符
            writer.writerow([s.encode('utf-8') if not isinstance(s, int) and not isinstance(s, float) else s for s in values_list])
