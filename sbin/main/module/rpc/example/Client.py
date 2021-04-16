#!/usr/bin/env python

from thrift import Thrift
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from module.rpc.handler import Handler

try:
    # Make socket
    transport = TSocket.TSocket('localhost', 54295)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = Handler.Client(protocol)

    # Connect!
    transport.open()

    # a = client.send(u"2\u0001869b6878-07e9-44c5-a56b-43dab7e0cc7e\u0002123")
    a = client.send(u"1\u0001application_1231231231")
    print(a)
    # Close!
    transport.close()

except Thrift.TException, tx:
    print '%s' % (tx.message)
