package com.mob.dataengine.commons.enums

object OperatorEnum extends Enumeration {

  type OperatorType = Value

  val UNICOM: OperatorType = Value(0, "unicom") // 全国联通
  val HENAN_MOBILE: OperatorType = Value(1, "henan_mobile") // 河南移动
  val SHANDONG_MOBILE: OperatorType = Value(2, "shandong_mobile") // 山东移动
  val SICHUAN_MOBILE: OperatorType = Value(3, "sichuan_mobile") // 四川移动
  val JIANGSU_MOBILE: OperatorType = Value(4, "jiangsu_mobile") // 江苏移动
  val ANHUI_MOBILE: OperatorType = Value(5, "anhui_mobile") // 安徽移动
  val GUANGDONG_MOBILE: OperatorType = Value(6, "guangdong_mobile") // 广东移动
  val ZHEJIANG_MOBILE: OperatorType = Value(7, "zhejiang_mobile") // 浙江移动
  val TIANJIN_MOBILE: OperatorType = Value(8, "tianjin_mobile") // 天津移动
  val HEBEI_MOBILE: OperatorType = Value(9, "hebei_mobile") // 河北移动


}
