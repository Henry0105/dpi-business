package com.youzu.mob.java.udf;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author juntao zhang
 */
public class DecodeHex extends UDF {

  public byte[] evaluate(String str) throws DecoderException {
    if (StringUtils.isBlank(str)) {
      return null;
    }
    return Hex.decodeHex(str.toCharArray());
  }
}
