package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.utils.FileUtils
import com.mob.dataengine.engine.core.mapping.IdMappingV2
import org.apache.spark.sql.LocalSparkSession
import org.json4s.DefaultFormats
import org.scalatest.FunSuite
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization._
import org.json4s.JsonDSL._

/**
 * @author juntao zhang
 */
class IdMappingV2Test extends FunSuite with LocalSparkSession with UDFCollections {
  implicit val formats: DefaultFormats.type = DefaultFormats
  val json: String = FileUtils.getJson("unittest/mapping/id_mapping_v2.json")

  val jobCommon: JobCommon = new JobCommon("jobId", "jobName", "rpcHost", 0, "day")

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")

    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database rp_dataengine")

    val dataOPTCacheNewSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val dataOPTCacheSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)

    val imeiMappingSql = FileUtils.getSqlScript( s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_imei_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3)
    createTable(imeiMappingSql)

    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
  }

  def prepareWarehouse(): Unit = {
    CSVUtils.loadDataIntoHive(spark, "../data/mapping/data_opt_cache",
      PropUtils.HIVE_TABLE_DATA_OPT_CACHE, sep = ":")
    CSVUtils.loadDataIntoHive(spark, "../data/mapping/dm_imei_mapping",
      PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3, ":")
  }

  test("idmappingv2 test") {
    IdMappingV2.main(Array(json))

    val resCnt = spark.sql(
      s"""
         |select id
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid2'
      """.stripMargin).count()

    assert(resCnt == 5)

    val nullCnt = spark.sql(
      s"""
        |select *
        |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
        |where uuid = 'it_uuid2' and match_ids is not null and match_ids[4] is  null
      """.stripMargin).count()

    assert(nullCnt == 0)

    val noQuoteCnt = spark.sparkContext.textFile("tmp/output_id_mapping_v2_it_uuid/*.csv").map(line => {
      val ar = line.split("\t")
      val arlen = ar.length
      (ar, arlen)
    }).filter(row => row._2 == 1).count()
    assertResult(1)(noQuoteCnt)
  }

  test("idmappingv2 匹配输出多条") {
    IdMappingV2.main(Array(json))

    val res = spark.sql(
      s"""
         |select match_ids[4] devices
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid2' and id = 'a0000037de1b3c'
      """.stripMargin).map(_.getString(0)).collect().head.split(",")

    assert(res.size == res.intersect(Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6f")).length)

    IdMappingV2.idmappingV2Job.stat("job_id")
    val matchInfo = IdMappingV2.idmappingV2Job.matchInfo
    assert(matchInfo.matchCnt == 4)
    assert(matchInfo.idCnt == 5)
    assert(matchInfo.outCnt.m("device_cnt") == 5)
  }

  test("idmappingv2 默认匹配1条") {
    val newJson = writePretty(JsonMethods.parse(json).removeField{
      case ("matchLimit", x) => true
      case _ => false
    })

    IdMappingV2.main(Array(newJson))
    val res = spark.sql(
      s"""
         |select match_ids[4] devices
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid2' and id = 'a0000037de1b3c'
      """.stripMargin).map(_.getString(0)).collect().head.split(",")

    assert(res.size == res.intersect(Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661")).length)

    IdMappingV2.idmappingV2Job.stat("job_id")
    val matchInfo = IdMappingV2.idmappingV2Job.matchInfo
    assert(matchInfo.matchCnt == 4)
    assert(matchInfo.idCnt == 5)
    assert(matchInfo.outCnt.m("device_cnt") == 4)
  }

  test("idmappingv2 match_limit=-1 匹配所有") {
    val newJson = writePretty(JsonMethods.parse(json).mapField{
      case ("matchLimit", x) => ("matchLimit", -1)
      case x => x
    })

    IdMappingV2.main(Array(newJson))

    val res = spark.sql(
      s"""
         |select match_ids[4] devices
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid2' and id = 'a0000037de1b3c'
      """.stripMargin).map(_.getString(0)).collect().head.split(",")

    assert(res.size == res.intersect(Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6e",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6f")).length)

    IdMappingV2.idmappingV2Job.stat("job_id")
    val matchInfo = IdMappingV2.idmappingV2Job.matchInfo
    assert(matchInfo.matchCnt == 4)
    assert(matchInfo.idCnt == 5)
    assert(matchInfo.outCnt.m("device_cnt") == 6)
  }

  test("idmappingv2 limit=-1 导出所有") {
    val newJson = writePretty(JsonMethods.parse(json).removeField{
      case ("limit", x) => true
      case _ => false
    }.mapField{
      case ("matchLimit", x) => ("matchLimit", -1)
      case x => x
    })

//    val inputs = Array(new ParamInput(uuid = None, name = None, value = "", idType = Some(1), sep = Some(",")))
//    val outputs = Array(IdMappingV2.ParamOutput(idTypes = Seq(4), encryptType = 0,
//      uuid = "it_uuid", hdfsOutput = "/tmp/hdfs_output", limit = None, matchLimit = Some(-1)))
//    val param = Param(inputs, outputs)
//
//    val idmappingV2Job = IdMappingV2Job(spark, inputDF, DeviceType.IMEI,
//      DummyEncrypt(), "imei", "imei", param.getOutputsFieldsWithKey().mkString(","), outputs, Some("="))
//    idmappingV2Job.submit()

    IdMappingV2.main(Array(newJson))

    val res = spark.sql(
      s"""
         |select match_ids[4] devices
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid2' and id = 'a0000037de1b3c'
      """.stripMargin).map(_.getString(0)).collect().head.split(",")

    assert(res.size == res.intersect(Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6e",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6f")).length)

    IdMappingV2.idmappingV2Job.stat("job_id")
    val matchInfo = IdMappingV2.idmappingV2Job.matchInfo
    assert(matchInfo.matchCnt == 4)
    assert(matchInfo.idCnt == 5)
    assert(matchInfo.outCnt.m("device_cnt") == 6)
  }
}
