package org.apache.spark.metrics;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * @author juntao zhang
 */
public class MetricsTest {

  private static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern.compile(
      "^([0-9-]+)\\s([^\\s]+)\\s[^\\s]\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+)\\s" +
          "([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)\\s([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)" +
          "(\\s[0-9-]+){15}");

  public static void main(String[] args) {
//    pid();
//    directMem();
    //rss: 进程独占内存+共享库，单位pages，125610
    /*
    RES：resident memory usage 常驻内存
      1、进程当前使用的内存大小，但不包括swap out
      2、包含其他进程的共享
      3、如果申请100m的内存，实际使用10m，它只增长10m，与VIRT相反
      4、关于库占用内存的情况，它只统计加载的库文件所占内存大小
     */
    String line = "22543 (java) T 22542 22542 15790 34870 4123 4202496 108858 17278 0 0 1261 73 36 8 20 0 42 0 1875396906 22220591104 125610 18446744073709551615 4194304 4196468 140733551453184 140733551435728 227673145517 0 0 0 16800975 18446744073709551615 0 0 17 20 0 0 0 0 0";

    Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(line);
    if (m.find()) {
      // Set (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
      System.out.println("name=" + m.group(2));
      System.out.println("ppid=" + m.group(3));
      System.out.println("pgrpId=" + m.group(4));
      System.out.println("session=" + m.group(5));
      System.out.println("utime=" + m.group(7));
      System.out.println("stime=" + m.group(8));
      System.out.println("vsize=" + m.group(10));
      System.out.println("rss=" + Long.valueOf(m.group(11)) * 2048);
    }
  }

  private static void directMem() {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = null;
    try {
      objectName = new ObjectName("java.nio:type=BufferPool,name=direct");
      MBeanInfo info = mbs.getMBeanInfo(objectName);
      for (MBeanAttributeInfo i : info.getAttributes()) {
        System.out.println(i.getName() + ":" + mbs.getAttribute(objectName, i.getName()));
      }
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 1024);
      byteBuffer.put(new byte[1024 * 1024]);
      System.out.println("===");
      info = mbs.getMBeanInfo(objectName);
      for (MBeanAttributeInfo i : info.getAttributes()) {
        System.out.println(i.getName() + ":" + mbs.getAttribute(objectName, i.getName()));
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void pid() {
    String name = ManagementFactory.getRuntimeMXBean().getName();
    // get pid
    String pid = name.split("@")[0];
    System.out.println("Pid is:" + pid);
  }
}
