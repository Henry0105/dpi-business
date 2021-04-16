package com.mob.dataengine.utils

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object SparkUtils {
  def main(args: Array[String]): Unit = {
    val jobType = args(0)   // 任务类型
    val jobParams = args(1) // json参数, 可以解析为map

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"SparkUtils_" + args(0))
      .enableHiveSupport()
      .getOrCreate()

    val params = parse2Map(jobParams)

    val method = this.getClass.getMethod(jobType, classOf[SparkSession], classOf[Map[String, String]])
    method.invoke(SparkUtils, spark, params)
  }

  // scalastyle:off classforname
  def companion[T](name : String)(implicit man: Manifest[T]) : T = {
    Class.forName(name + "$").getField("MODULE$").get(man.runtimeClass).asInstanceOf[T]
  }
  // scalastyle:on classforname

  // 将传入的json参数, 解析成map结构
  def parse2Map(jsonStr: String): Map[String, String] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    JsonMethods.parse(jsonStr).extract[Map[String, String]]
  }

  def createView(spark: SparkSession, params: Map[String, String]): Unit = {
    createView(spark, params("table"), params("versionVal"), params("versionName"))
  }

  // 这个是已有代码,不做修改
  def createView(spark: SparkSession, tableName: String, versionVal: String, versionName: String = "version"): Unit = {
    val schema = spark.table(tableName).schema

    val fields = schema.fields.map(f => {
      f.name
    }).filterNot(_.equals(versionName))

    val sql =
      s"""
         |create or replace view ${tableName}_view
         |(
         | ${fields.mkString(",")}
         |) COMMENT ''
         |as select
         |  ${fields.mkString(",")}
         |from $tableName where $versionName='$versionVal'
    """.stripMargin
    println(sql)
    spark.sql(sql)
  }
}
