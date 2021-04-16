# coding=utf-8
import os
os.environ['MID_ENGINE_ENV'] = 'local'
os.environ['DATAENGINE_HOME'] = '../../../../../'


import json
import logging
import unittest
import shutil
from module.job.job import RpcJob
from thrift import Thrift
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from module.rpc.handler import Handler
from module.tools.env import dataengine_env


class TestJob(RpcJob):
    def __init__(self, job):
        RpcJob.__init__(self, job)

    def receive(self, rpc_param):
        RpcJob.receive(self, rpc_param)
        print("receive: ", rpc_param)
        return True

    def prepare(self):
        RpcJob.prepare(self)
        print("call prepare in TestJob")

    def run(self):
        print("call run in TestJob")

    def upload(self):
        print("call upload in TestJob")


def send_info(host, port):
    try:
        # Make socket
        transport = TSocket.TSocket(host, port)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        # Create a client to use the protocol encoder
        client = Handler.Client(protocol)

        # Connect!
        transport.open()

        # a = client.send(u"2\u0001869b6878-07e9-44c5-a56b-43dab7e0cc7e\u0002123")
        print('start send')
        a = client.send(u"1\u0001faf")
        print(a)
        # Close!
        transport.close()

    except Thrift.TException, tx:
        print '%s' % (tx.message)
        import traceback
        print(traceback.format_exc())


if __name__ == "__main__":
    job = TestJob({"job_id": "job_id", "job_name": "job_name"})
    job.submit()
    print(job.job)
    send_info('localhost', job.job['rpc_port'])
    target_dir = "%s/%s/%s/%s" % (dataengine_env.dataengine_data_home, "job_id", str(1), 'uuid')
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    os.makedirs(target_dir)
    os.chdir(target_dir)
    print(os.getcwd())
