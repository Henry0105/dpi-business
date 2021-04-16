#!/usr/bin/env python
from time import sleep

from module.rpc import RpcServer, Iface


################################
#    this is a server demo     #
################################


class RPCImplDemo(Iface):
    def __init__(self):
        pass

    def send(self, param):
        print(param)
        return True


t = RpcServer(RPCImplDemo())
t.start()
print(t.port)
sleep(1000)
