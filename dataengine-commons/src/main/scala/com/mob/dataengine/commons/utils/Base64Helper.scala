package com.mob.dataengine.commons.utils

import java.nio.charset.StandardCharsets
import java.util.Base64

object Base64Helper {

  def encode(str: String): String = {
    Base64.getEncoder.encodeToString(str.getBytes(StandardCharsets.UTF_8))
  }

  def decode(str: String): String = {
    val bytes = Base64.getDecoder.decode(str)
    new String(bytes, StandardCharsets.UTF_8)
  }

}
