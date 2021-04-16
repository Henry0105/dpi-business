package com.mob.dataengine.commons.utils

import java.io.UnsupportedEncodingException
import java.security.{MessageDigest, NoSuchAlgorithmException}

object Sha256Helper {

  def entrySHA256(str: String): String = {
    var encodeStr = ""
    try {
      val messageDigest = MessageDigest.getInstance("SHA-256")
      messageDigest.update(str.getBytes("UTF-8"))
      encodeStr = byte2Hex(messageDigest.digest())
    } catch {
      case e1: NoSuchAlgorithmException =>
        e1.printStackTrace()
      case e2: UnsupportedEncodingException =>
        e2.printStackTrace()
    }
    encodeStr
  }

  def byte2Hex(bytes: Array[Byte]): String = {
    val stringBuffer = new StringBuffer()
    var temp: String = ""
    for (i <- bytes.indices) {
      temp = Integer.toHexString(bytes(i) & 0xFF)
      if (temp.length == 1) {
        stringBuffer.append("0")
      }
      stringBuffer.append(temp)
    }
    stringBuffer.toString
  }
}
