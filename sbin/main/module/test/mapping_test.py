# coding=utf-8
import os
print(os.getcwd())
os.environ['MID_ENGINE_ENV'] = 'local'
os.environ['DATAENGINE_HOME'] = '../../../../'

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
from module.events.mapping import IdMapping
from module.project import Bootstrap

def send_info(host, port, msg):
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
        a = client.send(msg)
        print(a)
        # Close!
        transport.close()

    except Thrift.TException, tx:
        print '%s' % (tx.message)
        import traceback
        print(traceback.format_exc())


class IdMappingTest(unittest.TestCase):
    def test_submit(self):
        param = {
            'job_id': 'id_mapping_test',
            'job_name': 'id_mapping',
            'param': [
                {
                    "outputs": [
                        {
                            "uuid": "uuid_11"
                        },
                        {
                            "uuid": "uuid_12"
                        }
                    ]
                },
                {
                    "outputs": [
                        {
                            "uuid": "uuid_2"
                        }
                    ]
                }
            ]
        }

        idmapping = IdMapping(param)
        idmapping.prepare()

        host = idmapping.job['rpc_host']
        port = idmapping.job['rpc_port']
        send_info(host, port, u'1\u0001application_121')
        send_info(host, port, u'2\u0001uuid_1\u0002{"uuid":"uuid_1", "dfa":12}')
        send_info(host, port, u'2\u0001uuid_2\u0002{"uuid":"uuid_2", "dfa":12}')
        # idmapping.receive(u'2\u0001uuid_1\u0002{"dfa":12}')
        print(idmapping.job_msg)
        print(idmapping.job_msg.to_json())
        print(json.dumps(idmapping.job_msg.to_json()))
        print(idmapping.outputs)
        # Bootstrap().run(param)


if __name__ == '__main__':
    unittest.main()