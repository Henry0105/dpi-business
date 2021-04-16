package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.utils.PropUtils

object ProfileConstants {

  /**
   * 作业配置相关
   */

  // 配置文件名称
  lazy val PROPERTIES_FILE_NAME: String = "application.properties"
  // dfs系统下载路径
  lazy val DFS_PATH: String = ""
  // 分隔符
  lazy val EXPORT_DATA_SEPARATOR = "\u0001"
  // 输出文件数量为1
  lazy val EXPORT_DATA_BLOCK_NUM_ONE = 1
  // 输出文件数量为10
  lazy val EXPORT_DATA_BLOCK_NUM_TEN = 10

  // 输出文件数量100
  lazy val EXPORT_DATA_BLOCK_NUM_HUNDRED = 100
  // 重分区数量
  lazy val SPARK_SQL_SHUFFLE_PARTITIONS = 1200
  // 集群id
  // mysql JDBC连接信息
  lazy val JDBC_MYSQL_IP: String = ""
  lazy val JDBC_MYSQL_PORT: String = ""
  lazy val JDBC_MYSQL_DATABASE: String = ""
  lazy val JDBC_MYSQL_USER: String = ""
  lazy val JDBC_MYSQL_PASSWORD: String = ""

  lazy val HIVE_TABLE_SCORE: String = PropUtils.HIVE_TABLE_MOBEYE_O2O_BASE_SCORE_DAILY
  lazy val HIVE_TABLE_APP_INFO: String = PropUtils.HIVE_TABLE_APPINFO_DAILY

  lazy val DEFAULT_DAY = "20180000"
}
