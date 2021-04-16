package com.mob.dataengine.commons.utils

import org.scalatest.FlatSpec

/**
 * @author juntao zhang
 */
class Md5HelperTest extends FlatSpec {
  "Md5Helper" should "entryMD5_32" in {
    assert(Md5Helper.entryMD5_32("zhang juntao") == "38cb7f21b23bfca7236032ac16fa7086")
    println(Md5Helper.entryMD5_32("zhang juntao"))
    println(Md5Helper.encryptMd5_32("zhang juntao"))
  }
}
