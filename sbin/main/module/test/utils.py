# coding=utf-8
import json
import os
import unittest
import mock
current_dir = os.getcwd()
print('current_dir is: ', current_dir)
os.environ['MID_ENGINE_ENV'] = 'local'
os.environ['DATAENGINE_HOME'] = current_dir[0:current_dir.find('sbin')]

from module.tools.hive import Hive
from module.tools.notify import MQ


def load_json(*paths):
    home = os.environ.get('DATAENGINE_HOME')
    tmp = ["unittest"]
    tmp.extend(paths)
    path = os.path.join(home, *tmp)
    with open(path) as f:
        data = json.loads(f.read())
    return data


def side_effect(*args, **kwargs):
    print("call side")
    return mock.DEFAULT


class MockRpcServer:
    def __init__(self, host="", port=0):
        self.host = host
        self.port = port


class MockSpark:
    def __init__(self):
        self.rpc_server = MockRpcServer()


class TestJob(unittest.TestCase):
    def setUp(self):
        dfs_patch = mock.patch('module.tools.dfs.fast_dfs.download_with_decrypt')
        self.addCleanup(dfs_patch.stop)
        self.mock_dfs = dfs_patch.start()

        spark_patch = mock.patch('module.job.job.Spark2')
        self.addCleanup(spark_patch.stop)
        spark_klass = spark_patch.start()
        self.spark = spark_klass.return_value
        self.spark.rpc_server = MockRpcServer()
        self.spark.submit.side_effect=side_effect

        mq_patch = mock.patch.object(MQ, 'send2')
        self.addCleanup(mq_patch.stop)
        self.mq_send2 = mq_patch.start()
        self.mq_send2.return_value = 0

        hive_patch = mock.patch.object(Hive, 'submit')
        self.addCleanup(hive_patch.stop)
        self.hive_submit = hive_patch.start()
        self.hive_submit.return_value = 0