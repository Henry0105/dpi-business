# coding=utf-8
import time
import traceback
import logging

__author__ = 'zhangjt'

from module.const.version import __version__ as version
from module.tools import notify, hbase, cfg_parser, spark_utils, apppkg2vec_utils, crowd_app_time
from module.tools.ext_label_merge import ExtLabelMerge
from module.tools.hive import DataEngine
from module.tools.hive2kafka import Hive2Kafka
from module.tools.http import HttpClient
from module.events import lookalike, portrait, idmapping, tags, \
    tags_integration, crowd, filter, mapping, mapping_v2, profile_export, encrypt_parse, edd_utils, \
    multidimensional_filter, phone_operator_mapping, data_cleaning, tags_hfile, apppkg2vec, ext_profile, \
    tags_hfile_incr, ios_tags_generator, pid_xid, business_dpi_mkt_url
from module.events.pca_tfidf import PCATfidf
from module.events.profile_cal import *
from module.job import job
from module.tools.idmapping_pid_bk import IdMappingPhoneBk, IdMappingPidBk


class JobContext:
    def __init__(self, url, task_id):
        self.url = url
        self.start_time = time.time()
        self.job_id = 'job_id'
        self.job_name = 'job_name'
        self.task_id = int(task_id)
        self.status = 0
        try:
            self.job = self.get_job_params()
        except Exception, e:
            self.status = -1
            self.exception = traceback.format_exc(1000)
            self.end_job()
            # notify.Notifier().send(self)
            notify.Notifier().send2http(self, url="http://scheduler.paas.internal.mob.com/api/writeTaskResult")
            raise e

        self.job_id = str(self.job['job_id']) if 'job_id' in self.job else str(self.job['jobId'])
        self.job_name = str(self.job['job_name']) if 'job_name' in self.job else str(self.job['jobName'])
        self.status = 0
        self.exception = None
        self.end_time = -1
        logging.info("%s[%s] is beginning..." % (self.job_id, self.job_name))
        self.job_duration_desc = None

    # req获取json
    def get_job_params(self):
        data = HttpClient().get(self.url)
        resp = json.loads(data)
        logging.info("%s response: %s" % (self.url, json.dumps(resp, indent=1)))
        return resp

    def end_job(self):
        self.end_time = time.time()
        m, s = divmod(self.end_time - self.start_time, 60)
        h, m = divmod(m, 60)
        self.job_duration_desc = ("job duration cost %02dh%02dm%02ds" % (h, m, s))

    def submit_job(self, jobs):
        job_status = job.JobStatus(0, job.JobMsg(), None)
        try:
            job_status = jobs[self.job_name](self.job).submit()
            if job_status.exception is not None:
                self.exception = str(job_status.exception)
        except Exception, e:
            self.exception = traceback.format_exc(1000)
            job_status.job_msg.merge(job.DataEngineException(e.message).to_dict())
        finally:
            logging.info("send http with: " + json.dumps(job_status.job_msg.to_json()))
            # notify.MQ().send2(subject=dataengine_env.dataengine_job_mq, content=job_status.job_msg.to_json())
            # 调接口
            callback_url = "http://scheduler.paas.internal.mob.com/api/writeTaskResult"
            r = HttpClient()
            rp_body = json.dumps({
                "task_id": self.task_id,
                "task_result": json.dumps(job_status.job_msg.to_json())
            })
            logging.info("rp_body with: " + rp_body)
            r.post(callback_url,
                   data=rp_body,
                   timeout=200,
                   headers={"Content-Type": "application/json"})
            if self.end_time is -1:
                self.end_job()
            logging.info(self)
            logging.info(
                "%s[%s] is ended, %s" % (self.job_id, self.job_name, self.job_duration_desc))
            if self.exception is not None:
                # notify.Notifier().send(self)
                notify.Notifier().send2http(self, callback_url)
                exit(1)

    def __str__(self):
        s = '\n'.join(['%s\t=>\t%s' % (
            key,
            value if key is not "job" else (json.dumps(value, indent=2)[0:1000])
        ) for (key, value) in self.__dict__.items()])
        return "\n===============job context===============\n%s\n==============================\n\n" % s


class SingleJob:
    def __init__(self, url, bootstrap, task_id):
        self.bootstrap = bootstrap
        self.cxt = JobContext(url, task_id)
        self._init_jobs()

    # 初始化jobs TODO 需要使用注释方式 自动注入
    def _init_jobs(self):
        self.jobs = {

            "crowd_portrait_calculation": portrait.CrowdCalculation,
            "crowd_portrait_estimation": portrait.CrowdEstimation,
            "crowd_portrait_adjuster": portrait.CrowdAdjust,
            "profile_cal_score": ProfileCalScore,
            "profile_cal_app_info": ProfileCalAppInfo,
            "profile_cal_app_info_v2": ProfileCalAppInfoV2,
            "profile_cal_frequency": ProfileCalFrequency,
            "profile_cal_source_flow": ProfileCalSourceAndFlow,
            "profile_cal_batch_monomer": ProfileCalBatchMonomer,
            "profile_cal_group": ProfileCalGroup,
            "profile_export_back_track": profile_export.ProfileBatchBackTracker,

            "location_device_mapping": mapping.LocationMapping,
            "location_device_mapping_v2": mapping.LocationMappingV2,
            "id_mapping": mapping.IdMapping,
            "id_mapping_v2": mapping_v2.IdMappingV2,
            "id_mapping_v3": mapping_v2.IdMappingV3,
            "id_mapping_v4": mapping_v2.IdMappingV4,
            "phone_operator_mapping": phone_operator_mapping.PhoneOperatorMapping,
            "pid_operator_mapping": phone_operator_mapping.PidOperatorMapping,
            "data_encrypt_parse": encrypt_parse.DataEncryptionDecodingLaunch,
            "data_encrypt_parse_v2": encrypt_parse.DataEncryptionDecodingLaunchV2,
            "data_encrypt_parse_v3": encrypt_parse.DataEncryptionDecodingLaunchV3,
            "data_check_edd": edd_utils.EddUtils,

            "crowd_selection_optimization": crowd.SelectionOptimization,
            "crowd_set_operation": filter.CrowdSetOperation,
            "crowd_set_operation_v2": filter.CrowdSetOperationV2,
            "crowd_filter": filter.CrowdFilter,
            "crowd_app_time_filter": filter.CrowdAppTimeFilter,

            "lookalike_normal": lookalike.Normal,
            "multidimensional_filter": multidimensional_filter.MultidimensionalFilter,
            "data_cleaning": data_cleaning.DataCleaning,
            "apppkg_similarity": apppkg2vec.ApppkgSimilarity,
            "carrier_profile": ext_profile.ExternalProfile,
            "jiguang_profile": ext_profile.ExternalProfile,
            "getui_profile": ext_profile.ExternalProfile,

            "business_dpi_mkt_url": business_dpi_mkt_url.BusinessDpiMktUrl
        }

    def submit(self):
        self.cxt.submit_job(self.jobs)


class Bootstrap:
    usage = "Usage: python driver.py <json_url>"

    def __init__(self):
        # todo deprecated
        self.dataengine_home = dataengine_env.dataengine_home
        self.dataengine_lib_path = dataengine_env.dataengine_lib_path
        self.dataengine_conf = dataengine_env.dataengine_conf
        self.pom_version = dataengine_env.pom_version
        self.dataengine_hdfs_data_home = dataengine_env.dataengine_hdfs_data_home
        self.dataengine_data_home = dataengine_env.dataengine_data_home
        self.hdfs_namespace = dataengine_env.hdfs_namespace
        # todo delete hdfs_home
        self.hdfs_home = dataengine_env.hdfs_namespace
        self.my_java_home = dataengine_env.my_java_home
        # self.mail_receiver = dataengine_env.mail_receiver
        self.dataengine_db_name = dataengine_env.dataengine_db_name
        self.dataengine_job_mq = dataengine_env.dataengine_job_mq

        self.simple_job = None

    def tools(self, args):
        if len(args) is 0:
            args = ["-h"]
        from module.const.argparse import ArgumentParser
        parser = ArgumentParser()
        parser.add_argument('-v', '--version', action='version', version=version)
        parser.add_argument('--create_hive_table', action='store_true', default=False,
                            help='创建dataengine库表,目前创建通过运维')
        parser.add_argument('--upload_dfs', action='store_true', default=False, help='上传文件')
        parser.add_argument('--device_tags', action='store_true', default=False, help='标签导入hbase')
        parser.add_argument('--ios_device_tags', action='store_true', default=False, help='ios标签导入hbase')
        parser.add_argument('--tags_tool', action='store_true', default=False, help='新标签生成工具')
        parser.add_argument('--tags_integration_tool', action='store_true', default=False, help='新标签生成工具')
        parser.add_argument('--id_tags_latest', action='store_true', default=False,
                            help='device,imei,mac,phone,标签导入hive')
        parser.add_argument('--idmapping', action='store_true', default=False)
        parser.add_argument('--idmapping_v2', action='store_true', default=False)
        parser.add_argument('--idmapping_sec', action='store_true', default=False)
        parser.add_argument('--snsuid_mapping', action='store_true', default=False)
        parser.add_argument('--drop_hbase_table', help='删除 [%s] hbase表' % cfg_parser.get("hbase_cluster"),
                            action='store_true', default=False)
        parser.add_argument('--pcatfidf', action='store_true', default=False, help='pca_tfidf计算')
        parser.add_argument('--pcatfidfV2', action='store_true', default=False, help='pca_tfidf计算新版本')
        parser.add_argument('--spark_utils', action='store_true', default=False, help='spark_utils')
        parser.add_argument('--tags_v2', action='store_true', default=False, help='带回溯带tagsv2')
        parser.add_argument('--tags_hfile', action='store_true', default=False, help='根据tag生成hfile')
        parser.add_argument('--tags_hfile_incr', action='store_true', default=False, help='根据tag生成hfile 每日增量')
        parser.add_argument('--hive2kafka', action='store_true', default=False, help='hive数据导出到kafka')
        parser.add_argument('--icon2vec', action='store_true', default=False, help='游戏竞品图标特征计算')
        parser.add_argument('--apppkg2vector_preparation', action='store_true', default=False,
                            help='游戏竞品特征筛选聚合计算, 包括app|icon|detail')
        parser.add_argument('--apppkg2vector_top_similarity', action='store_true', default=False,
                            help='游戏竞品相似度预计算')
        parser.add_argument('--idmapping_phone_bk', action='store_true', default=False,
                            help='idmapping phone =>device 可回溯数据')
        parser.add_argument('--idmapping_pid_bk', action='store_true', default=False,
                            help='idmapping pid =>device 可回溯数据')
        parser.add_argument('--ext_lable_merge', action='store_true', default=False,
                            help='数据交换离线画像合并')
        parser.add_argument('--ios_tags_generator_sec', action='store_true', default=False, help='合规ios画像生成 ')
        parser.add_argument('--crowd_app_time', action='store_true', default=False, help='时间维度app状态筛选表生成')
        parser.add_argument('--pid_xidLabel', action='store_true', default=False, help='pid到xidlabel映射表生成')
        parser.add_argument('--pid_xid', action='store_true', default=False, help='pid到xid映射表')
        namespace, other_args = parser.parse_known_args(args=args)
        logging.info(namespace)
        logging.info(other_args)

        if namespace.create_hive_table:
            DataEngine().create_not_exist()
        elif namespace.upload_dfs:
            fast_dfs.upload2(other_args)
        elif namespace.device_tags:
            tags.DeviceTags().submit(other_args)
        elif namespace.ios_device_tags:
            tags.IosDeviceTags().submit(other_args)
        elif namespace.tags_tool:
            tags.TagsTool().submit(other_args)
        elif namespace.tags_integration_tool:
            tags_integration.TagsIntegerationTool().submit(other_args)
        elif namespace.id_tags_latest:
            tags.IdTagsLatest().submit(other_args)
        elif namespace.idmapping:
            idmapping.Tools().submit(other_args)
        elif namespace.idmapping_v2:
            idmapping.ToolsV2().submit(other_args)
        elif namespace.idmapping_sec:
            idmapping.ToolsSec().submit(other_args)
        elif namespace.snsuid_mapping:
            idmapping.SnsuidMapping(other_args).submit()
        elif namespace.drop_hbase_table:
            hbase.drop_hbase_table(other_args)
        elif namespace.pcatfidf:
            PCATfidf(other_args).submit()
        elif namespace.pcatfidfV2:
            PCATfidf(other_args).submit2()
        elif namespace.spark_utils:
            spark_utils.SparkUtils().submit(other_args)
        elif namespace.tags_v2:
            tags_hfile.TagsHFile().gen_hive_data(other_args)
        elif namespace.tags_hfile:
            tags_hfile.TagsHFile().gen_hfile(other_args)
        elif namespace.tags_hfile_incr:
            tags_hfile_incr.TagsHFileIncr().gen_hfile(other_args)
        elif namespace.hive2kafka:
            Hive2Kafka(other_args).submit()
        # elif namespace.icon2vec:
        #     apppkg2vec_utils.Icon2Vec(other_args).run()
        elif namespace.apppkg2vector_preparation:
            apppkg2vec_utils.Apppkg2Vec(other_args).preparation_cal()
        elif namespace.apppkg2vector_top_similarity:
            apppkg2vec_utils.Apppkg2Vec(other_args).top_similarity_cal()
        elif namespace.idmapping_phone_bk:
            IdMappingPhoneBk(other_args).submit()
        elif namespace.idmapping_pid_bk:
            IdMappingPidBk(other_args).submit()
        elif namespace.ext_lable_merge:
            ExtLabelMerge(other_args).submit()
        elif namespace.ios_tags_generator_sec:
            ios_tags_generator.IosTagsGeneratorSec(other_args).submit()
        elif namespace.crowd_app_time:
            crowd_app_time.CrowdAppTime(other_args).submit()
        elif namespace.pid_xidLabel:
            pid_xid.PidXidLabel().submit(other_args)
        elif namespace.pid_xid:
            pid_xid.pidXid(other_args).start_up()
        else:
            logging.error('unrecognized arguments: %s' % ' '.join(other_args))
            parser.print_help()

    def run(self, url, task_id):
        self.simple_job = SingleJob(url, self, task_id)
        self.simple_job.submit()