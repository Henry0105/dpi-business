package example;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author juntao zhang
 */
public class KryoTest {

  static class User {
    public List list = new ArrayList();

  }

  public static void main(String[] args) {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Output output = new Output(outputStream);
    int i = 4000000;
    User user = new User();
    long start = System.currentTimeMillis();
    while (i-- > 0) {
      user.list.add(UUID.randomUUID().toString());
    }
    kryo.writeClassAndObject(output, user);
    output.close();
    System.out.println((System.currentTimeMillis()-start)/1000);
    byte[] bytes = outputStream.toByteArray();

    System.out.println("===============" + bytes.length);

//    System.out.println(new String(bytes));
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    Input input = new Input(inputStream);
    User copy = (User)kryo.readClassAndObject(input);
    input.close();
    System.out.println(user);
    System.out.println(copy);
  }

}
