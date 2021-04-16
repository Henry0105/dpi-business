package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.commons.enums.BusinessEnum.BusinessType
import com.mob.dataengine.commons.utils.JdbcTools
import com.mob.dataengine.utils.PropUtils

object Jdbcs {

  def of(businessEnum: BusinessType): JdbcTools = {
    val businessName = businessEnum.toString
    val ip: String = PropUtils.getProperty(s"$businessName.mysql.ip")
    val port: Int = PropUtils.getProperty(s"$businessName.mysql.port").toInt
    val user: String = PropUtils.getProperty(s"$businessName.mysql.user")
    val pwd: String = PropUtils.getProperty(s"$businessName.mysql.password")
    val db: String = PropUtils.getProperty(s"$businessName.mysql.database")
    of(ip, port, user, pwd, db)
  }

  def of(jdbcHostname: String, jdbcPort: Int, jdbcUsername: String, jdbcPassword: String,
         jdbcDatabase: String): JdbcTools = {
    JdbcTools(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)
  }

}
