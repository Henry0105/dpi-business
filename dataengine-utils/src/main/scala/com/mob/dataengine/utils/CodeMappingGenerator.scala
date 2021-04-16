package com.mob.dataengine.utils

import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.traits.Logging
import org.apache.spark.sql.SparkSession
import com.mob.dataengine.commons.utils.{PropUtils => UP}
import org.apache.spark.sql.functions._

object CodeMappingGenerator  {
  /**
   * 表设计思路
   * http://c.mob.com/pages/viewpage.action?pageId=37784120
   */
  def main(args: Array[String]): Unit = {
    val day = args(0)
    val idMax = args(1).toInt
    val encryptMax = args(2).toInt

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    CodeMappingGenerator(spark).run(day, idMax, encryptMax)
  }
}

case class CodeMappingGenerator(spark: SparkSession) extends Logging {
  /**
   * @param day 分区日期
   * @param idMax 支持的idmapping类型的最大值,从1开始
   * @param encryptMax 支持的加密类型的最大值,从0开始
   */
  def run(day: String, idMax: Int, encryptMax: Int): Unit = {
    val fields = spark.table(UP.HIVE_TABLE_DATAENGINE_CODE_MAPPING).schema.fieldNames.dropRight(1)
    // seed
    val seedDF = sql(
      s"""
         |select 'seed', 'seed', '种子包', 'seed'
        """.stripMargin).toDF(fields: _*)
    // id数据
    val idMapping = Seq(
      ("1", "imei"),
      ("2", "mac"),
      ("3", "phone"),
      ("4", "device"),
      ("5", "imei_14"),
      ("6", "imei_15"),
      ("7", "idfa"),
      ("8", "imsi"),
      ("9", "serialno"),
      ("10", "oaid"))

    val encryptMapping = Seq(
      ("0", ""),
      ("1", "_md5"),
      ("2", "_aes"),
      ("3", "_md516"),
      ("4", "_sha256")
    )
    val product = for {
      id <- idMapping
      encryptId <- encryptMapping
    } yield {
      (id, encryptId)
    }

    val idDF = spark.createDataFrame(
      product.map{ tuple =>
      val (id, en) = tuple._1
      val (encryptId, encryptEn) = tuple._2
      ((id.toInt + encryptId.toInt * 100).toString, s"$en$encryptEn", s"$en$encryptEn", s"$id,$encryptId")
      }
    ).toDF(fields: _*)

    val profileDF = spark.createDataFrame(MetadataUtils.findProfileIdMobIdAndCn())
      .withColumn("query", lit(null)).toDF(fields: _*)


    seedDF.union(idDF).union(profileDF).createOrReplaceTempView("tmp")
    sql(
      s"""
         |insert overwrite table ${UP.HIVE_TABLE_DATAENGINE_CODE_MAPPING} partition(day='20200502')
         |select *
         |from tmp
        """.stripMargin)
  }
}