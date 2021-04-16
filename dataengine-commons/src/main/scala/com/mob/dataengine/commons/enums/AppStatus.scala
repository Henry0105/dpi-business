package com.mob.dataengine.commons.enums

object AppStatus extends Enumeration {

  type AppStatus = Value

  val uninstall: AppStatus = Value(-1, "uninstall")
  val installed: AppStatus = Value(0, "installed")
  val newInstall: AppStatus = Value(1, "new_install")
  val active: AppStatus = Value(3, "active")

  def withId(id: Int): AppStatus = {
    id match {
      case -1 => uninstall
      case 0 => installed
      case 1 => newInstall
      case 3 => active
    }
  }
}
