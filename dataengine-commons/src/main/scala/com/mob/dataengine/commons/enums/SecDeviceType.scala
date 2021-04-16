package com.mob.dataengine.commons.enums

object SecDeviceType extends Enumeration {
  type SecDeviceType = Value
  val NOTYPE: SecDeviceType = Value(0, "noType")
  val IEID: SecDeviceType = Value(1, "ieid")
  val MCID: SecDeviceType = Value(2, "mcid")
  val PID: SecDeviceType = Value(3, "pid")
  val DEVICE: SecDeviceType = Value(4, "device")
  val IEID14: SecDeviceType = Value(5, "ieid_14")
  val IEID15: SecDeviceType = Value(6, "ieid_15")
  val IFID: SecDeviceType = Value(7, "ifid")
  val ISID: SecDeviceType = Value(8, "isid")
  val SNID: SecDeviceType = Value(9, "snid")
  val OIID: SecDeviceType = Value(10, "oiid")
  val IEID_LTM: SecDeviceType = Value(101, "ieid_ltm")
  val MCID_LTM: SecDeviceType = Value(102, "mcid_ltm")
  val PID_LTM: SecDeviceType = Value(103, "pid_ltm")
  val DEVICE_LTM: SecDeviceType = Value(104, "device_ltm")
  val IEID14_LTM: SecDeviceType = Value(105, "ieid_14_ltm")
  val IEID15_LTM: SecDeviceType = Value(106, "ieid_15_ltm")
  val IFID_LTM: SecDeviceType = Value(107, "ifid_ltm")
  val ISID_LTM: SecDeviceType = Value(108, "isid_ltm")
  val SNID_LTM: SecDeviceType = Value(109, "snid_ltm")
  val OIID_LTM: SecDeviceType = Value(110, "oiid_ltm")
}
