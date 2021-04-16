package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.enums.DeviceType
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.{DataFrame, LocalSparkSession}
import com.mob.dataengine.commons.utils.{AesHelper, PropUtils}
import com.mob.dataengine.engine.core.mapping.IdMapping
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.lit

/**
 * @author juntao zhang
 */
class IdMappingTest extends FunSuite with LocalSparkSession {
  val json: String = FileUtils.getJson(s"unittest/mapping/id_mapping.json")

  var inputDF: DataFrame = _

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")

    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database rp_dataengine")

    inputDF = Seq(
      "a0000037ddf832",
      "a0000037de0616",
      "a0000037de087f",
      "a0000037de1b3c",
      "a0000037de1bsc"
    ).toDF("data")

    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
  }

  def prepareWarehouse(): Unit = {
    Seq(
      ("a0000037ddf832", Seq("cdf7464d664816d83adb983ff45bc4e6bbc70bf0"), Seq("20150731"), "20190320"),
      ("a0000037de0616", Seq("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c"), Seq("20150731"), "20190320"),
      ("a0000037de087f", Seq("04399d01c2e2fac798f4f1b47285185ebbdca738"), Seq("20150731"), "20190320"),
      ("a0000037de1b3c",
        Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661", "340dd1242a75d8b596175d7c17f552c09cd6ce6e",
          "340dd1242a75d8b596175d7c17f552c09cd6ce6f"),
        Seq("20160731", "20151031", "20151231"), "20190320")
    ).toDF(
      "imei", "device", "device_tm", "day"
    ).write.partitionBy("day").saveAsTable(PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3)

    inputDF.withColumn("created_day", lit("20190101"))
      .withColumn("biz", lit("4|2"))
      .withColumn("uuid", lit("input_uui1"))
      .write.partitionBy("created_day", "biz", "uuid").saveAsTable("rp_dataengine.data_opt_cache")
  }

  test("idmapping test") {
    IdMapping.main(Array(json))
    val res = spark.sql(
      s"""
         |select data
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = 'id_mapping_uuid3'
      """.stripMargin).map(_.getString(0)).collect()

    assert(res.intersect(Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6f",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6e"
    ).map(k => AesHelper.encodeAes("v4lRvcJJV1zW5Tcs", "1236966788039762", k))).length == 3)

    val matchInfo = IdMapping.idmappingJob.matchInfoTmp
    assert(matchInfo.matchCnt == 4)
    assert(matchInfo.idCnt == inputDF.count())

    val idCounts = IdMapping.idmappingJob.idCounts
    assert(idCounts(DeviceType.DEVICE) == 6)
  }
}
