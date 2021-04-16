package com.youzu.mob.java.udf;

import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author juntao zhang
 */
public class ConcatWs extends UDF {

  public String evaluate(ArrayList<String> iter) {
    if (iter == null) {
      return null;
    }
    return StringUtils.join(iter, ',');
  }
}
