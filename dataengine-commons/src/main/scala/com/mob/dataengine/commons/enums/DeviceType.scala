package com.mob.dataengine.commons.enums

/**
 * @author juntao zhang
 */

object DeviceType extends Enumeration {
  type DeviceType = Value
  val NOTYPE: DeviceType = Value(0, "noType")
  val IMEI: DeviceType = Value(1, "imei")
  val MAC: DeviceType = Value(2, "mac")
  val PHONE: DeviceType = Value(3, "phone")
  val DEVICE: DeviceType = Value(4, "device")
  val IMEI14: DeviceType = Value(5, "imei_14")
  val IMEI15: DeviceType = Value(6, "imei_15")
  val IDFA: DeviceType = Value(7, "idfa")
  val IMSI: DeviceType = Value(8, "imsi")
  val SERIALNO: DeviceType = Value(9, "serialno")
  val OAID: DeviceType = Value(10, "oaid")
  val IMEI_LTM: DeviceType = Value(101, "imei_ltm")
  val MAC_LTM: DeviceType = Value(102, "mac_ltm")
  val PHONE_LTM: DeviceType = Value(103, "phone_ltm")
  val DEVICE_LTM: DeviceType = Value(104, "device_ltm")
  val IMEI14_LTM: DeviceType = Value(105, "imei_14_ltm")
  val IMEI15_LTM: DeviceType = Value(106, "imei_15_ltm")
  val IDFA_LTM: DeviceType = Value(107, "idfa_ltm")
  val IMSI_LTM: DeviceType = Value(108, "imsi_ltm")
  val SERIALNO_LTM: DeviceType = Value(109, "serialno_ltm")
  val OAID_LTM: DeviceType = Value(110, "oaid_ltm")
}
