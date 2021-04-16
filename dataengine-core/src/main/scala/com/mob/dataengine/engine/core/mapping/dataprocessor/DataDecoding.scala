package com.mob.dataengine.engine.core.mapping.dataprocessor

import com.mob.dataengine.commons.DeviceCacheWriter
import com.mob.dataengine.commons.enums.JobName
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class DataDecoding (
    @transient spark: SparkSession,
               fieldName: String,
               idType: Int,
               outputDay: String,
               outputUuid: String,
               limit: Long,
               hdfsOutputPath: String,
               jobName: String,
    @transient sourceData: DataFrame
) {

  @transient private var decodingDF: DataFrame = _

  def getDecodingDF(): DataFrame = {
    decodingDF
  }

  def submit(): Unit = {
    decodeId()
    insertHive()
    hdfsOutput()
  }

  def decodeId(): Unit = {

    val idType = fieldName

    val resultDF =
      idType match {
        case "imei" => decodeImei(sourceData, rainbowTableName = PropUtils.HIVE_TABLE_TOTAL_IMEI_MD5_MAPPING)
        case "mac" => decodeMac(sourceData, rainbowTableName = PropUtils.HIVE_TABLE_TOTAL_MAC_MD5_MAPPING)
        case "phone" => decodePhone(sourceData, rainbowTableName = PropUtils.HIVE_TABLE_TOTAL_PHONE_MD5_MAPPING)
        case _ => null
      }

    this.decodingDF = resultDF
  }

  private def decodeImei(df: DataFrame, rainbowTableName: String): DataFrame = {
    df.createOrReplaceTempView("base_tab1")
    spark.sql(
      s"""
         |SELECT
         |    concat(split(b.imei,',')[0], a.imei) as imei
         |FROM
         |    ${rainbowTableName} a
         |JOIN
         |    base_tab1 b
         |ON
         |    a.md5_imei = split(b.imei,',')[1]
       """.stripMargin)
  }

  private def decodeMac(df: DataFrame, rainbowTableName: String): DataFrame = {
    df.createOrReplaceTempView("base_tab1")
    import spark.implicits._
    spark.sql(
      s"""
         |SELECT
         |    concat(split(b.mac,',')[0],a.mac) as mac
         |FROM
         |    ${rainbowTableName} a
         |JOIN
         |    base_tab1 b
         |ON
         |    a.md5_mac=split(b.mac, ',')[1]
       """.stripMargin
    ).map(row => {
      val mac = row.getAs[String]("mac")
      val s = new StringBuffer()
      for(i <- 0 until mac.length) {
        s.append(s"${mac(i)}")
        if (i > 0 && (i + 1)%2 == 0) s.append(":")
      }
      s.substring(0, s.length()-1)
    }).toDF(colNames = "mac")
  }

  private def decodePhone(df: DataFrame, rainbowTableName: String): DataFrame = {
    df.createTempView("base_tab1")
    spark.sql(
      s"""
         |SELECT
         |    a.phone
         |FROM
         |    ${rainbowTableName} a
         |JOIN
         |    base_tab1 b
         |ON
         |    a.md5_phone = b.phone
       """.stripMargin)
  }

  def insertHive(): Unit = {

    val biz = s"${JobName.getId(jobName)}|${idType}"
    val paramOutput = new com.mob.dataengine.commons.pojo.ParamOutput(uuid = Some(outputUuid),
      value = "", outputType = None, day = Some(outputDay))

    DeviceCacheWriter.insertTable(spark, output = paramOutput,
      dataset = this.decodingDF.withColumnRenamed(fieldName, newName = "data"), biz = biz)
  }

  def hdfsOutput(): Unit = {
    this.decodingDF.createOrReplaceTempView("base_tab")

    val biz = s"${JobName.getId(jobName)}|${idType}"

    val resultDF = spark.sql(
      s"""
         |SELECT
         |    ${fieldName}
         |FROM
         |    base_tab
         |LIMIT
         |    ${limit}
       """.stripMargin
    )

    resultDF.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header", "true").option("sep", "\t")
      .csv(hdfsOutputPath)
  }
}
