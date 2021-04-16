package example;

import com.mob.dataengine.commons.utils.Md5Helper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author juntao zhang
 */
public class IdMappingGetTest {

  private static Logger logger = LoggerFactory.getLogger(IdMappingGetTest.class);

  public static void main(String[] args) {
    // String table = "default:rp_phone_mapping_20180908";
    // String table = "default:rp_imei_mapping_20180908";
    // String table = "default:rp_imei_14_mapping_20180908";
    String table = "default:rp_device_mapping_20180908";
    // String table = "default:rp_fin_tianqi_loan_pre_app_score";

    System.setProperty("HADOOP_USER_NAME", "dba");
    Connection connection = null;

    try {
      connection = getConnection();
      TableName tableName = TableName.valueOf(table);
      long t = System.currentTimeMillis();
      if (args.length > 0) {
        String path = args[0];
        List<String> lines = init(path);
        // for (String rowkey : lines) {
          get(connection, tableName, lines);
        // }
      } else {
        String rowkey = "00000000abba1531ada4924254e8493177c96012";
        List<String> rowkeyList = new ArrayList<String>();
        rowkeyList.add(rowkey);
        get(connection, tableName, rowkeyList);
      }

      System.out.println((System.currentTimeMillis() - t) / 1000);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        assert connection != null;
        connection.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  private static List<String> init(String path) {
    BufferedReader in = null;
    List<String> lines = new ArrayList<String>();
    try {
      in = new BufferedReader(new FileReader(path));
      lines = IOUtils.readLines(in);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(in);
    }
    return lines;
  }

  private static Connection getConnection() throws IOException {
    return ConnectionFactory.createConnection(getConfiguration());
  }

  private static Configuration getConfiguration() throws IOException {
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.property.clientPort", "2181");
    // config.set("hbase.zookeeper.quorum", "bd15-101.yzdns.com,bd15-130.yzdns.com,bd15-102.yzdns.com");//test
    config.set("hbase.zookeeper.quorum", "bd15-098,bd15-099,bd15-107");
    // config.set("hbase.zookeeper.quorum", "bd15-161-218,bd15-161-220,bd15-161-219"); // BlkHbaseCluster
    return config;
  }

  public static void get(Connection connection, TableName tableName, List<String> rowKeys)
      throws IOException, DecoderException {
    try (Table table = connection.getTable(tableName)) {
      List<Get> getList = new ArrayList<>(rowKeys.size());
      for (String rowKey : rowKeys) {
        // Get get = new Get(Hex.decodeHex(Md5Helper.encryptMd5_32(rowKey).toCharArray())); // imei,mac,phone=>device
        Get get = new Get(Hex.decodeHex(rowKey.toCharArray())); // device=>imei,mac,phone
        getList.add(get);
      }

      Result[] results = table.get(getList);
      for (int j = 0; j < results.length; j++) {
        String rowKey = rowKeys.get(j);
        Result result = results[j];
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = result.getMap();
        if (null != navigableMap) {
          for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()) {
            // logger.info("columnFamily:{}", Bytes.toString(entry.getKey()));
            NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
            int size = map.size();
            int i = 1;
            System.out.print(/*"device#" + */rowKey + "#");
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> en : map.entrySet()) {
              String sep = i == size ? "" : "#";
              String field = Bytes.toString(en.getKey());
              // System.out.print(field + "#");
              NavigableMap<Long, byte[]> nm = en.getValue();
              for (Map.Entry<Long, byte[]> me : nm.entrySet()) {
                if ("updateTime".contains(field)) {
                  System.out.print(Bytes.toLong(me.getValue()) + sep);
                } else if ("score".contains(field)) {
                  System.out.print(Bytes.toDouble(me.getValue()) + sep);
                } else {
                  System.out.print(Bytes.toString(me.getValue()) + sep);
                }
              }
              i++;
            }
            System.out.println();
          }
        }
      }
    }
  }
}
