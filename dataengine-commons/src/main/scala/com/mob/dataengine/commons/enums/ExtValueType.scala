package com.mob.dataengine.commons.enums

import com.mob.dataengine.commons.enums.EncryptType.EncryptType

/**
 * @author : xlmeng
 * @date : 2020/4/30
 */
object ExtValueType extends Enumeration {

  type ExtValueType = Value

  val PHONE: ExtValueType = Value(0, "phone")
  val PHONEMD5: ExtValueType = Value(1, "phonemd5")
  val PHONESHA256: ExtValueType = Value(2, "phonesha256")

  val IMEI: ExtValueType = Value(3, "imei")
  val IMEI14: ExtValueType = Value(4, "imei14")
  val IMEI15: ExtValueType = Value(5, "imei15")
  val IMEIMD5: ExtValueType = Value(6, "imeimd5")

  val MAC: ExtValueType = Value(7, "mac")
  val MACMD5: ExtValueType = Value(8, "macmd5")

  val IMSI: ExtValueType = Value(9, "imsi")
  val IMSIMD5: ExtValueType = Value(10, "imsimd5")

  val IDFA: ExtValueType = Value(11, "idfa")

  private val MD5 = Set(PHONEMD5, IMEIMD5, MACMD5)
  private val SHA156 = Set(PHONESHA256)

  def isMD5(a: ExtValueType): Boolean = MD5.contains(a)

  def isSHA256(a: ExtValueType): Boolean = SHA156.contains(a)

  def determineEncryptType(a: ExtValueType): EncryptType = {
    if (isMD5(a)) {
      EncryptType.MD5_32ENCRYPT
    } else if (isSHA256(a)) {
      EncryptType.SHA256ENCRYPT
    } else {
      EncryptType.NONENCRYPT
    }
  }

}
