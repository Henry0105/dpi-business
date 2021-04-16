# coding=utf-8
import logging.config
import threading

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

from module.rpc.handler import Handler
from module.rpc.handler.Handler import Iface
from module.tools import shell

class _TMobServer(TServer.TServer):
    """Simple single-threaded server that just pumps around one transport."""

    def __init__(self, *args):
        TServer.TServer.__init__(self, *args)
        self.serverTransport.listen()
        # self.host = (self.serverTransport.handle.getsockname()[0])
        self.port = (self.serverTransport.handle.getsockname()[1])

    def serve(self):
        while True:
            client = self.serverTransport.accept()
            if not client:
                continue
            itrans = self.inputTransportFactory.getTransport(client)
            otrans = self.outputTransportFactory.getTransport(client)
            iprot = self.inputProtocolFactory.getProtocol(itrans)
            oprot = self.outputProtocolFactory.getProtocol(otrans)
            try:
                while True:
                    self.processor.process(iprot, oprot)
            except TTransport.TTransportException, tx:
                pass
            except Exception, x:
                logging.exception(x)

            itrans.close()
            otrans.close()


class RpcServer(threading.Thread):
    logger = logging.getLogger(__name__)

    def __init__(self, rpc_impl):
        status, ip = shell.submit_with_stdout("hostname -i")
        self.host = ip.strip()
        super(RpcServer, self).__init__()
        processor = Handler.Processor(rpc_impl)
        self.server = _TMobServer(processor,
                                  TSocket.TServerSocket(self.host, port=0),
                                  TTransport.TBufferedTransportFactory(),
                                  TBinaryProtocol.TBinaryProtocolFactory())
        self.port = self.server.port
        while not self.isDaemon():
            self.setDaemon(True)

    def run(self):
        logging.info('Starting the server...')
        self.server.serve()
        logging.info('done.')


class AbstractRpcHandler(Iface):
    def __init__(self):
        pass

    # rpc receive from spark job
    def send(self, param):
        logging.info("rpc receive param => %s" % param)
        try:
            return self.receive(param)
        except Exception, e:
            logging.exception(e)
            return False

    def receive(self, param):
        pass
