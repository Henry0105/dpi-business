package com.mob.dataengine.utils.iostags.helper

import java.util.Properties

import com.mob.dataengine.utils.PropUtils
import com.mob.dataengine.utils.iostags.beans.IosProfileInfo
import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xlmeng
 */
object MetaDataHelper {
  /** 单体标签mysql表 */
  val individualProfile = "t_individual_profile"
  /** 标签元信息表 */
  val profileMetadata = "t_profile_metadata"
  /** 标签分类表 */
  val profileCategory = "t_profile_category"
  /** 置信度表 */
  val profileConfidence = "t_profile_confidence"
  /** 输出标签表 */
  val labelOutput = "label_output"
  /** 输出标签画像表 */
  val labelOutputProfile = "label_output_profile"
  /** 标签版本表 */
  val profileVersion = "t_profile_version"

  private val ip: String = PropUtils.getProperty("tag.mysql.ip")
  private val port: Int = PropUtils.getProperty("tag.mysql.port").toInt
  private val user: String = PropUtils.getProperty("tag.mysql.user")
  private val pwd: String = PropUtils.getProperty("tag.mysql.password")
  private val db: String = PropUtils.getProperty("tag.mysql.database")
  private val url: String = s"jdbc:mysql://$ip:$port/$db?useUnicode=true&amp;characterEncoding=UTF-8?autoReconnect=true"

  val properties: Properties = new Properties()
  properties.setProperty("user", user)
  properties.setProperty("password", pwd)
  properties.setProperty("driver", "com.mysql.jdbc.Driver")

  private def createViewByMysql(spark: SparkSession): Unit = {
    Seq(individualProfile, labelOutput, labelOutputProfile, profileMetadata, profileCategory, profileVersion,
      profileConfidence)
      .foreach(tb =>
        spark.read.jdbc(url, tb, properties).createOrReplaceTempView(tb)
      )

    import org.apache.spark.sql.functions._
    spark.table(individualProfile)
      .withColumn("profile_table", when(col("profile_table") === "ios_device_info_full_view",
        "ios_device_info_sec_df").otherwise(col("profile_table")))
      .createOrReplaceTempView(individualProfile)
  }

  /**
   * 从mysql中读取对应标签信息
   */
  private def readFromMySQl(spark: SparkSession): DataFrame = {
    sql(spark,
      s"""
         |SELECT c.profile_id, c.profile_version_id
         |FROM (
         |      SELECT id
         |      FROM $labelOutput
         |      WHERE status in (3,5,6,7,8,9)
         |     ) a
         |JOIN $labelOutputProfile b
         | ON a.id = b.output_id
         |JOIN $profileVersion c
         | ON c.is_avalable = 1 AND c.is_visible = 1 AND b.profile_version_id = c.id
         |GROUP BY c.profile_id, c.profile_version_id
         |""".stripMargin).createOrReplaceTempView("meta1")

    sql(
      spark,
      s"""
         |SELECT a.profile_id
         |     , a.profile_version_id
         |     , a.profile_database
         |     , a.profile_table
         |     , a.profile_column
         |     , a.has_date_partition
         |     , a.profile_datatype
         |     , a.period     --更新频率：day,week,month,year
         |     , a.period_day --频率对应的日期,周几,一个月第几天
         |     , a.update_type
         |     , a.partition_type
         |FROM   $individualProfile as a
         |INNER JOIN meta1 as b
         |ON a.profile_id = b.profile_id AND a.profile_version_id = b.profile_version_id
         |INNER JOIN $profileMetadata as d
         |ON a.profile_id = d.profile_id
         |INNER JOIN $profileCategory as c
         |ON d.profile_category_id = c.profile_category_id
         |WHERE a.os = 'iOS' AND a.profile_datatype IN ('int', 'string', 'boolean', 'double', 'bigint')
         |""".stripMargin)
  }

  def parseIosProfileInfo(spark: SparkSession, df: DataFrame): Array[IosProfileInfo] = {
    import spark.implicits._
    val infos = df.map(r => IosProfileInfo(
      r.getAs[Int]("profile_id"),
      r.getAs[Int]("profile_version_id"),
      r.getAs[String]("profile_database"),
      r.getAs[String]("profile_table"),
      r.getAs[String]("profile_column"),
      r.getAs[Int]("has_date_partition"),
      r.getAs[String]("profile_datatype"),
      r.getAs[String]("period"),
      r.getAs[String]("period_day").toInt,
      r.getAs[String]("update_type"),
      r.getAs[String]("partition_type").toInt
    )).collect()
    infos.foreach(_.key = "ifid")
    infos
  }

  /**
   * 读取mysql中标签信息
   */
  def getComputedProfiles(spark: SparkSession, day: String, isFull: Boolean = false): Array[IosProfileInfo] = {
    // 带有版本号 1_1000,2_1000,3_1000
    createViewByMysql(spark)
    val metaDataDF = readFromMySQl(spark)
    val profiles = parseIosProfileInfo(spark, metaDataDF)

    if (isFull) {
      profiles
    } else {
      profiles.filter(p => day == TagsDateProcess.getProcessDate(day, p.period, p.periodDay))
    }
  }

  /**
   * 读取mysql中置信度信息
   */
  def getProfileConfidence(spark: SparkSession, day: String, full: Boolean): Array[IosProfileInfo] = {
    val metaDataDF = sql(spark,
      s"""
         |SELECT b.profile_id
         |     , b.profile_version_id
         |     , b.profile_database
         |     , b.profile_table
         |     , b.profile_column
         |     , b.has_date_partition
         |     , b.profile_datatype
         |     , b.period     --更新频率：day,week,month,year
         |     , b.period_day --频率对应的日期,周几,一个月第几天
         |     , b.update_type
         |     , b.partition_type
         |FROM $profileConfidence a
         |INNER JOIN $individualProfile b
         |ON b.os = 'iOS' AND a.profile_id = b.profile_id AND a.profile_version_id = b.profile_version_id
      """.stripMargin)
    parseIosProfileInfo(spark, metaDataDF)
  }


}
