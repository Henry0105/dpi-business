package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}

import java.util.Properties
import com.mob.dataengine.commons.utils.{AppUtils, PropUtils}
import com.mob.dataengine.commons.{DeviceSrcReader, JobCommon}
import com.mob.dataengine.engine.core.jobsparam.profilecal.ProfileBatchMonomerParam
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProfileJobCommon (
                         spark: SparkSession,
                         jobCommon: JobCommon,
                         param: ProfileBatchMonomerParam
                       ) extends Serializable {
  var matchCnt = 0L
  val (url, properties): (String, Properties) = initMysql()
  val profileIds: Seq[String] = param.profileIds
  val profileIdsBC: Broadcast[Seq[String]] = spark.sparkContext.broadcast(profileIds)

  def initMysql(): (String, Properties) = {
    var properties = new Properties()
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

  val deviceString: String = "device"
  val colList: List[String] = List(deviceString, "tags")
  var inputIDCount = 0L


  def getInputDF: DataFrame = {
    val hubService: DataHubService = DataHubServiceImpl(spark)
    val inputDF = param.inputs.map(input => DeviceSrcReader.toDFV2(spark, input, hubService = hubService))
      .reduce(_ union _)
      .withColumnRenamed("id", deviceString)
      .cache()
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

  def getFilteredDF(inputDF: DataFrame, tagsDF: DataFrame): DataFrame = {
    inputIDCount = inputDF.count()
    if (inputIDCount < 10000000) inputDF.cache()
    (if (inputIDCount > 10000000) { // 1kw
      tagsDF.join(inputDF, inputDF(deviceString) === tagsDF(deviceString), "inner")
    } else {
      tagsDF.join(broadcast(inputDF),
        inputDF(deviceString) === tagsDF(deviceString), "inner")
    }).drop(tagsDF(deviceString))
  }

  // key: profile_id + "_" + version_id + "_" + value
  def getProfileValueMap(spark: SparkSession, table: String): Map[String, String] = {
    import spark.implicits._
    spark.read.jdbc(url, table, properties)
      .filter("length(profile_value) > 0")
      .map(r => {
        val id = r.getAs[Int]("profile_id")
        val versionId = r.getAs[Int]("profile_version_id")
        val value = r.getAs[String]("profile_value").split(":") match {
          case Array(_, fullModel, _*) => fullModel
          case Array(v, _*) => v
        }  // HUAWEI:HUAWEI MT7-CL00
        val valueCn = r.getAs[String]("profile_value_cn") // 有些已经是中文的就不需要转化
        (s"${id}_${versionId}_$value", if (null == valueCn) value else valueCn)
      }).collect().toMap
  }

}
