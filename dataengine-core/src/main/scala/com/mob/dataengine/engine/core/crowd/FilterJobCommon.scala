package com.mob.dataengine.engine.core.crowd

import java.util.Properties

import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}
import com.mob.dataengine.commons.{DeviceSrcReader, JobCommon}
import com.mob.dataengine.commons.utils.{AppUtils, PropUtils}
import com.mob.dataengine.engine.core.jobsparam.MultidimensionalFilterParam
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

class FilterJobCommon(spark: SparkSession, jobCommon: JobCommon, param: MultidimensionalFilterParam)
  extends Serializable {

  var matchCnt = 0L
  val (url, properties): (String, Properties) = initMysql()
  val deviceString: String = "device"
  val colList: List[String] = List(deviceString, "tags")
  var inputIDCount = 0L

  def initMysql(): (String, Properties) = {
    val properties = new Properties()
    val ip: String = AppUtils.TAG_MYSQL_JDBC_IP
    val port: Int = AppUtils.TAG_MYSQL_JDBC_PORT
    val user: String = AppUtils.TAG_MYSQL_JDBC_USER
    val pwd: String = AppUtils.TAG_MYSQL_JDBC_PWD
    val db: String = AppUtils.TAG_MYSQL_JDBC_DB
    val url = s"jdbc:mysql://$ip:$port/$db?useUnicode=true&amp;characterEncoding=UTF-8?autoReconnect=true"

    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("driver", "com.mysql.jdbc.Driver")

    (url, properties)
  }

  def getInputDF: DataFrame = {
    val hubService: DataHubService = DataHubServiceImpl(spark)
    val inputDF = param.inputs.map(input => DeviceSrcReader.toDFV2(spark, input, None, None, isBk = false, hubService))
                  .reduce(_ union _)
    inputDF
  }

  def getTagsDF: DataFrame = {
    spark.sql(
      s"""
         |SELECT device, tags, confidence
         |FROM ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_VIEW}
         |WHERE tags is not null
       """.stripMargin)
  }

  def getInnerJoinDF(inputDF: DataFrame, tagsDF: DataFrame): DataFrame = {
    inputIDCount = inputDF.count()
    if (inputIDCount < 10000000) inputDF.cache()
    (if (inputIDCount > 10000000) { // 1kw
      tagsDF.join(inputDF, inputDF(deviceString) === tagsDF(deviceString), "inner")
    } else {
      tagsDF.join(broadcast(inputDF),
        inputDF(deviceString) === tagsDF(deviceString), "inner")
    }).drop(tagsDF(deviceString))
  }

}
