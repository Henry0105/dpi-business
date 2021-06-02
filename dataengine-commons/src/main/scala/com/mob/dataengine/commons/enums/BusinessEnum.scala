package com.mob.dataengine.commons.enums

object BusinessEnum extends Enumeration {

  type BusinessType = Value

  val dpi: BusinessType = Value(0, "dpi")
  val superadmin: BusinessType = Value(1, "superadmin")
  val marketplus: BusinessType = Value(2, "marketplus")
  val mobeye: BusinessType = Value(3, "mobeye")
  val ga: BusinessType = Value(4, "ga")
  val mobfin: BusinessType = Value(5, "mobfin")
  val di: BusinessType = Value(6, "di")
  val sjhz: BusinessType = Value(7, "sjhz")
//  val zy: BusinessType = Value(8, "zy")



  def withId(id: Int): BusinessType = {
    id match {
      case 1 => superadmin
      case 2 => marketplus
      case 3 => mobeye
      case 4 => ga
      case 5 => mobfin
      case 6 => di
      case 7 => sjhz
//      case 8 => zy
    }
  }

  def getChineseName(id: Int): String = {
    id match {
      case 1 => "超级管理员"
      case 2 => "智能增长线"
      case 3 => "商业地理"
      case 4 => "政府线"
      case 5 => "金融线"
      case 6 => "平台"
      case 7 => "数据合作"
//      case 8 => "智弈"
    }
  }


}
