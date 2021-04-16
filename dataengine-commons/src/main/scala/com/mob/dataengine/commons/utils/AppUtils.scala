package com.mob.dataengine.commons.utils

import java.io.InputStreamReader
import java.util.Properties

import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object AppUtils {
  private[this] lazy val logger: Logger = Logger.getLogger(getClass)

  private[this] lazy val prop: Properties = {
    val _prop = new Properties()
    val propFile = "application.properties"
    Try {
      val propIn = new InputStreamReader(getClass.getClassLoader.getResourceAsStream(propFile), "UTF-8")
      _prop.load(propIn)
      propIn.close()
    } match {
      case Success(_) =>
        logger.info(s"props loaded succeed from [$propFile], {${_prop}}")
        _prop
      case Failure(ex) =>
        logger.error(ex.getMessage, ex)
        throw new InterruptedException(ex.getMessage)
    }
  }

  private[this] def getProperty(key: String): String = prop.getProperty(key)

  // ********************* application.properties   **********************/
  lazy val TAG_MYSQL_JDBC_IP: String = getProperty("tag.mysql.ip")
  lazy val TAG_MYSQL_JDBC_PORT: Int = getProperty("tag.mysql.port").toInt
  lazy val DATAENGINE_HDFS_TMP: String = getProperty("dataengine.hdfs.tmp")
  lazy val TAG_MYSQL_JDBC_USER: String = getProperty("tag.mysql.user")
  lazy val TAG_MYSQL_JDBC_PWD: String = getProperty("tag.mysql.password")
  lazy val TAG_MYSQL_JDBC_DB: String = getProperty("tag.mysql.database")

  lazy val EXTERNAL_PROFILE_HOST: String = getProperty("external.profile.host")
  lazy val GIGUANG_PROFILEIDS: String = getProperty("giguang.profiles")
  lazy val GETUI_PROFILEIDS: String = getProperty("getui.profiles")

  lazy val PROFILE_BOUND_DAY: String = getProperty("profile.bound.day")
}
