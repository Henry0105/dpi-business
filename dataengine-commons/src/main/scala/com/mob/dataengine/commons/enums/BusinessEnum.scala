package com.mob.dataengine.commons.enums

object BusinessEnum extends Enumeration {

  type BusinessType = Value

  val marketplus: BusinessType = Value(1, "marketplus")
  val mobfin: BusinessType = Value(2, "mobfin")
  val mobeye: BusinessType = Value(3, "mobeye")
  val ga: BusinessType = Value(4, "ga")
  val dpi: BusinessType = Value(5, "dpi")

  def withId(id: Int): BusinessType = {
    id match {
      case 1 => marketplus
      case 2 => mobfin
      case 3 => mobeye
      case 4 => ga
      case 5 => dpi
    }
  }

  def getChineseName(id: Int): String = {
    BusinessEnum.withId(id) match {
      case marketplus => "智能增长线"
      case mobfin => "金融线"
      case mobeye => "商业地理"
      case ga => "政府线"
      case dpi => "dpi"
    }
  }


}
