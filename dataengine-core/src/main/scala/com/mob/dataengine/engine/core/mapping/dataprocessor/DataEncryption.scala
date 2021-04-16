package com.mob.dataengine.engine.core.mapping.dataprocessor

import com.mob.dataengine.commons.DeviceCacheWriter
import com.mob.dataengine.commons.enums.JobName
import com.mob.dataengine.commons.utils.Md5Helper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


case class DataEncryption (
    @transient spark: SparkSession,
               fieldName: String,
               idType: Int,
               outputDay: String,
               outputUuid: String,
               limit: Long,
               jobName: String,
               hdfsOutputPath: String,
    @transient sourceData: DataFrame
) {

  @transient private var encryptDF: DataFrame = _

  def submit(): Unit = {
    encrptId()
    insertHive()
    hdfsOutput()
  }

  def getEncryptDF(): DataFrame = {
    encryptDF
  }

  def encrptId(): Unit = {
    val idType = fieldName

    this.encryptDF =
      idType match {
        case "imei" => encryptImei(sourceData)
        case "mac" => encryptMac(sourceData)
        case "phone" => encryptPhone(sourceData)
        case _ => null
    }
  }

  private def encryptImei(value: DataFrame): DataFrame = {
    import spark.implicits._
    value.map(row => {
      val input = row.getAs[String](fieldName = "imei")
      val before = input.substring(0, 8)
      val after = input.substring(8)
      val md5After = Md5Helper.encryptMd5_32(after)
      s"${before},${md5After}"
    }).toDF("imei")
  }

  private def encryptMac(value: DataFrame): DataFrame = {
    import spark.implicits._
    value.map(row => {
      val input = row.getAs[String](fieldName = "mac")
      val data = input.replaceAll(":", "").toLowerCase()
      val before = data.substring(0, 6)
      val after = data.substring(6)
      val md5After = Md5Helper.encryptMd5_32(after)
      s"${before},${md5After}"
    }).toDF(colNames = "mac")
  }

  private def encryptPhone(value: DataFrame): DataFrame = {
    import spark.implicits._
    value.map(row => {
      val input = row.getAs[String](fieldName = "phone")
      val md5After = Md5Helper.encryptMd5_32(input)
      s"${md5After}"
    }).toDF("phone")
  }

  def insertHive(): Unit = {

    val biz = s"${JobName.getId(jobName)}|${idType}"

    val paramOutput = new com.mob.dataengine.commons.pojo.ParamOutput(uuid = Some(outputUuid),
      value = "", outputType = None, day = Some(outputDay))

    DeviceCacheWriter.insertTable(spark, output = paramOutput,
      dataset = this.encryptDF.withColumnRenamed(fieldName, newName = "data"), biz = biz)
  }

  def hdfsOutput(): Unit = {
    this.encryptDF.createOrReplaceTempView("base_tab")

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
