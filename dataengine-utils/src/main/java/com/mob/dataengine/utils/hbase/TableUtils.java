package com.mob.dataengine.utils.hbase;

import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author juntao zhang
 */
public class TableUtils {

  public static String intToHexString(int num) {
    char[] chs = new char[8];
    int index = chs.length - 1;

    for (int i = 0; i < 8; ++i) {
      int temp = num & 15;
      if (temp > 9) {
        chs[index] = (char) (temp - 10 + 97);
      } else {
        chs[index] = (char) (temp + 48);
      }

      --index;
      num >>>= 4;
    }

    return toString(chs);
  }

  public static String toString(char[] arr) {
    StringBuilder temp = new StringBuilder();

    for (char a : arr) {
      temp.append(a);
    }

    return temp.toString();
  }

  private static byte[] hexStringToBytes(String str) {
    if (str != null && !str.trim().equals("")) {
      if (str.length() % 2 != 0) {
        str = str + "0";
      }

      return Bytes.fromHex(str);
    } else {
      return new byte[0];
    }
  }

  public static byte[][] getMD5SplitKeys(int regionsNum, String type) {
    int splitKeysNumber = regionsNum - 1;
    byte[][] splitKeys = new byte[splitKeysNumber][];
    int tmp = regionsNum;

    int keyLength;
    for (keyLength = 1; (tmp >>>= 4) > 1; ++keyLength) {
      //ignore
    }

    int delim = (int) (Math.pow(16.0D, (double) keyLength) / (double) splitKeysNumber);
    int index = 0;

    for (int i = 0; (double) i < Math.pow(16.0D, (double) keyLength); ++i) {
      if (i != 0 && i % delim == 0) {
        String hexStr = intToHexString(i).substring(8 - keyLength, 8);
        System.out.println(hexStr);
        if (type.equals("str")) {
          splitKeys[index] = hexStr.getBytes();
        } else {
          splitKeys[index] = hexStringToBytes(hexStr);
        }

        ++index;
      }
    }

    return splitKeys;
  }

  public static void main(String[] args) throws DecoderException {
    byte[][] a = getMD5SplitKeys(256, "1");
    System.out.println(a);
  }
}
