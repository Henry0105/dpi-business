package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.mapping.{PhoneOperatorMapping}
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.json4s.DefaultFormats
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._


class PhoneOperatorMappingTest extends FunSuite with LocalSparkSession with UDFCollections {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val json: String = FileUtils.getJson("unittest/mapping/phone_operator_mapping.json")
  val jobCommon: JobCommon = new JobCommon("jobId", "jobName", "rpcHost", 0, "day")

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_marketplus_wisdom CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")

    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database dm_dataengine_tags")
    spark.sql("create database rp_dataengine")
    spark.sql("create database rp_marketplus_wisdom")
    spark.sql("create database dm_dataengine_test")
    spark.sql("create database rp_dataengine_test")

    val dataOPTCacheNewSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val dataOPTCacheSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)

    val tagsInfoSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_marketplus/call_phone_property.sql",
      tableName = PropUtils.HIVE_TABLE_ISP_MAPPING)
    createTable(tagsInfoSql)

    val codeMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/" +
      s"dm_dataengine_code_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
    createTable(codeMappingSql)

    val hubTable = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/profile/single_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_HUB)
    createTable(hubTable)

  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_marketplus_wisdom CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
  }

  def prepareWarehouse(): Unit = {
    val dataOptDF = Seq(
      "phone,day",
      "14797495532,20191113",
      "15874777180,20191112",
      "13911502070,20191110",
      "13101573135,20191112",
      "18856318212,20191111",
      "15757978377,20191111",
      "33333333333,20190101"
    ).toDF("data")

    insertDF2Table(dataOptDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20191111', biz='10|1', uuid='11328a45f49e6c545b715a1ad2831999'"))


    val hubDF = spark.sql(
      """
        | select '14797495532' as id, '20191111' as day, '移动' isp, '1' isp_code, '北京'   city_name, 'cn1_01' city_code,'' device, '' ltm
        | union all
        | select '18856318212' as id, '20191111' as day, '联通' isp, '2' isp_code, '北京'   city_name, 'cn1_01' city_code,'' device, '' ltm
        | union all
        | select '15757978377' as id, '20191111' as day, '电信' isp, '3' isp_code, '北京'   city_name, 'cn1_01' city_code,'' device, '' ltm
        | union all
        | select '13101573135' as id, '20191111' as day, '小灵通' isp, '4' isp_code, '北京' city_name, 'cn1_01' city_code,'' device, '' ltm
        | union all
        | select '15874777180' as id, '20191112' as day, '小灵通' isp, '4' isp_code, '北京' city_name, 'cn1_01' city_code,
        |   '12345678' as device, '20200617' as ltm
        |""".stripMargin).select(map(
      lit("seed"), array(col("id"), col("day")),
      lit("3"), array(col("id")
        , col("isp")
        , col("isp_code")
        , col("city_code")
        , col("city_name")),
      lit("4"), array(col("device"), col("ltm"))
    ).as("feature"))


    insertDF2Table(hubDF, PropUtils.HIVE_TABLE_DATA_HUB,
      Some("uuid='11328a45f49e6c545b715a1ad2831999'"))

    val ispPhoneDF = spark.sql(
      s"""
         |select "1479749" id, "电信" isp, 3 isp_code, "常州" city_name, "cn3_11" city_code
         |union all
         |select "1587477" id, "联通" isp, 2 isp_code, "西安" city_name, "cn18_06" city_code
         |union all
         |select "1310157" id, "联通" isp, 2 isp_code, "西安" city_name, "cn18_06" city_code
         |union all
         |select "1885631" id, "移动" isp, 1 isp_code, "上海" city_name, "cn1_01" city_code
         |union all
         |select "1575797" id, "移动" isp, 1 isp_code, "上海" city_name, "cn1_01" city_code
         |union all
         |select "1111111" id, "移动" isp, 1 isp_code, "上海" city_name, "cn1_01" city_code
         |union all
         |select "2222222" id, "联通" isp, 2 isp_code, "常州" city_name, "cn3_11" city_code
         |union all
         |select "3333333" id, "联通" isp, 1 isp_code, "西安" city_name, "cn18_06" city_code
       """.stripMargin).toDF("id", "isp", "isp_code", "city_name", "city_code")
    insertDF2Table(ispPhoneDF, PropUtils.HIVE_TABLE_ISP_MAPPING, Some("day='20191111'"))

    val codeMapping = spark.sql(
      """
        |select "2850_1000" as code,  "G21505_4_30" as en,  "30天内题材小类:中世纪安装个数"   as cn ,  "null"   as query
        | union all
        |select  "2851_1000" as code,  "G21102_4_30" as en,  "30天内题材小类:西方魔幻安装个数"  as cn ,  "null" as query
        | union all
        |select "2852_1000" as code,  "G21102_4_30" as en,  "30天内题材小类:西方魔幻安装个数"  as cn ,  "null" as query
        | union all
        |select "2853_1000" as code,  "G21102_4_30" as en,  "30天内题材小类:西方魔幻安装个数"  as cn ,  "null" as query
        | union all
        |select "2854_1000" as code,  "G21102_4_30" as en,  "30天内题材小类:西方魔幻安装个数"  as cn ,  "null" as query
        | union all
        |select "2855_1000" as code,  "G21002_4_30" as en,  "30天内题材小类:僵尸安装个数"    as cn ,    "null" as query
        |""".stripMargin)
    insertDF2Table(codeMapping, PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING, Some("day=20200613"))

  }

  test("phoneOperatorMapping_test") {
    prepareWarehouse()
    PhoneOperatorMapping.main(Array(json))

    val resCnt = spark.sql(
      s"""
         |select id
         |from
         |  ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = "hhhhhhhhhh"
       """.stripMargin).count()

//    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW).show(false)

    assertResult(2)(resCnt)

  }


}
