# coding=utf-8
import json
import logging
import os
import shutil
import uuid
import re


from module.const import main_class
from module.job.job_msg import JobMsg
from module.tools.cfg import prop_utils
from module.tools.check_cont import CheckCont, CheckType
from module.tools.dfs import fast_dfs
from module.tools.env import dataengine_env
from module.tools.opt import OptionParser
from module.tools.notify import MQ
from module.tools import shell
from module.tools.utils import get_limit
from module.job.job import RpcJob
from module.job.error import DataEngineException
from module.tools.dir_context import DirContext
from module.tools import utils
from module import tools

__author__ = 'zhangjt'


class ApppkgSimilarity(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)
        self.job_msg = JobMsg()
        self.job = job
        self.job_id = str(self.job['job_id']) if 'job_id' in self.job else str(self.job['jobId'])
        self.job_name = str(self.job['job_name']) if 'job_name' in self.job else str(self.job['jobName'])
        self.job_msg.put('job_id', self.job_id)
        self.job_msg.put('job_name', self.job_name)

    def receive(self, rpc_param):
        RpcJob.receive(self, rpc_param)
        event_code, rest = RpcJob.extract_code_and_rest(rpc_param)
        if event_code == "2":
            # {code}\u0001{uuid}\u0002{result_json}
            _, match_info = str.split(rest, u'\u0002')
            self.job_msg.put_into_details(json.loads(match_info))
        return True

    def run(self):
        self.spark2.submit(
            args='\'%s\'' % json.dumps(self.job, indent=1),
            job_name=self.job_name, job_id=self.job_id,
            props={
                "--class": main_class.APPPKG2VEC,
                "--driver-memory": "8g",
                "--executor-memory": "9g",
                "--executor-cores": "9"
            },
            conf={
                "spark.kryoserializer.buffer.max": "1024m"
            })
