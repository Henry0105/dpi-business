package example;

import com.mob.dataengine.rpc.thrift.Handler;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * @author juntao zhang
 */
public class RPCJavaClientExample {
    public static void main(String[] args) {
        try {
            TTransport transport = new TSocket("127.0.0.1", 52403);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            Handler.Client client = new Handler.Client(protocol);

            perform(client);

            transport.close();
        } catch (TException x) {
            x.printStackTrace();
        }
    }

    private static void perform(Handler.Client client) throws TException {
        boolean result = client.send("{\"name\":\"juntao\"}");
        System.out.println("Check result: " + result);
    }
}