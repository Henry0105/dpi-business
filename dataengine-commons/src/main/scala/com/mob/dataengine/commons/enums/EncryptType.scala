package com.mob.dataengine.commons.enums

object EncryptType extends Enumeration {

  type EncryptType = Value
  val NONENCRYPT: EncryptType = Value(0, "NonEncrypt")
  val MD5_32ENCRYPT: EncryptType = Value(1, "Md5_32ENCRYPT")
  val AESENCRYPT: EncryptType = Value(2, "AesENCRYPT")
  val MD5_16ENCRYPT: EncryptType = Value(3, "Md5_16ENCRYPT")
  val SHA256ENCRYPT: EncryptType = Value(4, "Sha256ENCRYPT")
}
