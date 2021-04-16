# coding=utf-8
import os
print(os.getcwd())
os.environ['MID_ENGINE_ENV'] = 'local'
os.environ['DATAENGINE_HOME'] = '../../../../../'


import json
import logging
import unittest
from module.job.job import RpcJob


class ErrorJob(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)

    def receive(self, rpc_param):
        print("receive: ", rpc_param)
        return True

    def prepare(self):
        raise Exception("prepare")

    def run(self):
        raise Exception("run exception")

    def upload(self):
        raise Exception("upload exception")


if __name__ == "__main__":
    ej = ErrorJob({"job_id": "job_id", "job_name": "job_name"})
    try:
        ej.submit()
    except Exception, e:
        print(e.message)
    finally:
        print("go here")