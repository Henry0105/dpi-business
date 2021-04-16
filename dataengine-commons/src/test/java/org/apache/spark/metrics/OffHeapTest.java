package org.apache.spark.metrics;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

/**
 * @author juntao zhang
 */
public class OffHeapTest {

  private long address = 0;

  private Unsafe unsafe = null;


  private byte[] bytes = null;

  public OffHeapTest() throws Exception {
    bytes = new byte[1024 * 1024 * 64];

    Field f = Unsafe.class.getDeclaredField("theUnsafe");
    f.setAccessible(true);
    unsafe = (Unsafe) f.get(null);
    address = unsafe.allocateMemory(1024 * 1024 * 1024);
    for (int i = 0; i < 1024 * 1024 * 64; i++) {
      unsafe.putLong(address, (long) i);
      address += 8;
    }
    System.out.println("end");

  }


  public static void main(String[] args) throws Exception {
    OffHeapTest heap = new OffHeapTest();
    System.out.println("memory address=" + heap.address);

    Thread.sleep(1000 * 10000);

  }

}
