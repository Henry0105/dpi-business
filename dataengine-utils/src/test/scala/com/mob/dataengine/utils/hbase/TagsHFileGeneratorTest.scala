package com.mob.dataengine.utils.hbase

import com.mob.dataengine.commons.utils.Md5Helper
import com.mob.dataengine.utils.tags.TagsHiveGenerator.Param
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.FlatSpec
/*
  * @author juntao zhang
  */
class TagsHFileGeneratorTest extends FlatSpec {
  "TagsHFileGenerator" should "json test" in {
    println(Param())
  }

  "TagsHFileGenerator" should "encodeHexString" in {
    println(Hex.encodeHexString(
      Bytes.toBytesBinary("\\x00\\xC1x\\x82\\xF7\\x9Ft\\xB3\\xB9\\x9B\\xB8\\xA4\\xBD\\x19N\\xCB\\xE0\\x99\\x97x")))
  }
  "TagsHFileGenerator" should "md5" in{
    println("22105d4adba40dcafc8e2550c7c0fbb3")
    println(Md5Helper.encryptMd5_32("862821031499789"))
  }
}
