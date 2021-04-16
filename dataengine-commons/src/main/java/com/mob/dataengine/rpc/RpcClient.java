package com.mob.dataengine.rpc;

import com.mob.dataengine.rpc.thrift.Handler;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * @author juntao zhang
 */
public class RpcClient {

  public static boolean send(String host, int port, String param) throws Exception {
    System.out.println("rpc param => " + param);
    try (TTransport transport = new TSocket(host, port)) {
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);
      Handler.Client client = new Handler.Client(protocol);
      return client.send(param);
    } catch (TException x) {
      throw new Exception(x);
    }
  }
}
