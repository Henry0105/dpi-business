package com.mob.dataengine.commons.utils

import java.io.UnsupportedEncodingException
import java.security.{InvalidAlgorithmParameterException, InvalidKeyException, NoSuchAlgorithmException}

import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.crypto.{BadPaddingException, Cipher, IllegalBlockSizeException, NoSuchPaddingException}
import org.apache.commons.codec.binary.Base64

object AesHelper {
  // 加密
  def encodeAes(_key: String, IV: String, value: String): String = {
    var result = ""
    try {
      val KEY_ALGORITHM = "AES"
      val CBC_CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding"
      val charset = "UTF-8"
      // 1. 实例化 AES编码器
      val cipher = Cipher.getInstance(CBC_CIPHER_ALGORITHM)
      // 3. 创建安全key对象
      val key = new SecretKeySpec(_key.getBytes, KEY_ALGORITHM)
      // 4. 创建初始化偏移量对象
      val ivParameterSpec = new IvParameterSpec(IV.getBytes(charset))
      // 5. 初始化AES编码器
      cipher.init(Cipher.ENCRYPT_MODE, key, ivParameterSpec)
      // 6. 执行编码操作
      val data = cipher.doFinal(value.getBytes(charset))
      // 7. base64编码
      result = Base64.encodeBase64String(data)
    } catch {
      case e: NoSuchAlgorithmException =>
        e.printStackTrace()
      case e: NoSuchPaddingException =>
        e.printStackTrace()
      case e: IllegalBlockSizeException =>
        e.printStackTrace()
      case e: BadPaddingException =>
        e.printStackTrace()
      case e: UnsupportedEncodingException =>
        e.printStackTrace()
      case e: InvalidAlgorithmParameterException =>
        e.printStackTrace()
    }
    result
  }

  // 解密
  def decodeAes(_key: String, IV: String, value: String): String = {
    var result = ""
    try {
      val KEY_ALGORITHM = "AES"
      val CBC_CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding"
      val charset = "UTF-8"
      // 1. base64解码
      val exidBytes = Base64.decodeBase64(value)
      // 2. 实例化 AES解码器
      val cipher = Cipher.getInstance(CBC_CIPHER_ALGORITHM)
      // 3. 创建安全key对象
      val key = new SecretKeySpec(_key.getBytes, KEY_ALGORITHM)
      // 4. 创建初始化偏移量对象
      val ivParameterSpec = new IvParameterSpec(IV.getBytes(charset))
      // 5. 初始化AES解码器
      cipher.init(Cipher.DECRYPT_MODE, key, ivParameterSpec)
      // 6. 执行解码操作
      result = new String(cipher.doFinal(exidBytes), charset)
    } catch {
      case e: NoSuchAlgorithmException => e.printStackTrace()
      case e: NoSuchPaddingException => e.printStackTrace()
      case e: InvalidKeyException => e.printStackTrace()
      case e: IllegalBlockSizeException => e.printStackTrace()
      case e: BadPaddingException => e.printStackTrace()
      case e: UnsupportedEncodingException => e.printStackTrace()
      case e: InvalidAlgorithmParameterException => e.printStackTrace()
    }
    result
  }
}
