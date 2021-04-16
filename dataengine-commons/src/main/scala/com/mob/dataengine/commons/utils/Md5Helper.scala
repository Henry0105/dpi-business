package com.mob.dataengine.commons.utils

/**
 * @author zhangnw
 */
object Md5Helper {

  import java.security.{MessageDigest, NoSuchAlgorithmException}

  def entryMD5_32(sourceStr: String): String = {
    var result = ""
    try {
      val md = MessageDigest.getInstance("MD5")
      md.update(sourceStr.getBytes)
      val b = md.digest
      var i = 0
      val buf = new StringBuffer("")
      var offset = 0
      while (offset < b.length) {
        i = b(offset)
        if (i < 0) i += 256
        if (i < 16) buf.append("0")
        buf.append(Integer.toHexString(i))

        offset += 1
        offset - 1
      }
      result = buf.toString
    } catch {
      case e: NoSuchAlgorithmException =>
        e.printStackTrace()
    }
    result
  }

  /**
   * MD5加密
   *
   * @param s 输入字符串
   * @return MD5字符串
   */
  def encryptMd5_32(s: String): String = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    val r = new java.math.BigInteger(1, m.digest()).toString(16)
    val sb = new StringBuffer()

    if (r.length == 32) {
      r
    }
    else {
      for (_ <- 0 until 32 - r.length) {
        sb.append("0")
      }
      sb.append(r)
      sb.toString
    }

  }

}
