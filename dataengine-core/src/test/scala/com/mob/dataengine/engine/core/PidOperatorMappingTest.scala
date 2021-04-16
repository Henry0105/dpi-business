package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.mapping.PidOperatorMapping
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.scalatest.FunSuite


class PidOperatorMappingTest extends FunSuite with LocalSparkSession with UDFCollections {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val json: String = FileUtils.getJson("unittest/mapping/pid_operator_mapping.json")
  val jobCommon: JobCommon = new JobCommon("jobId", "jobName", "rpcHost", 0, "day")

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS mobdi_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")

    spark.sql("create database mobdi_test")
    spark.sql("create database dm_sdk_mapping")
    spark.sql("create database dm_dataengine_test")
    spark.sql("create database rp_dataengine_test")
    spark.sql("create database rp_dataengine")

    val dataOPTCacheNewSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val dataOPTCacheSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)


    val areaSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/dm_sdk_mapping/city_level.sql",
      tableName = PropUtils.HIVE_TABLE_MAPPING_AREA_PAR)
    createTable(areaSql)

    val pidAttrSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/mobdi_test/attr.sql",
      tableName = PropUtils.HIVE_TABLE_DIM_PID_ATTRIBUTE_FULL_PAR_SEC)
    createTable(pidAttrSql)

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
    spark.sql("DROP DATABASE IF EXISTS mobdi_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
  }

  def prepareWarehouse(): Unit = {
    val dataOptDF = Seq(
      "pid1,20191113",
      "pid2,20191112",
      "pid3,20191110",
      "pid4,20191112",
      "pid5,20191111",
      "pid6,20191111",
      "3333333,20190101"
    ).toDF("data")

    insertDF2Table(dataOptDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20191111', biz='10|1', uuid='11328a45f49e6c545b715a1ad2831999'"))


    val hubDF = spark.sql(
      """
        | select 'pid1' as id, '20191111' as day, '移动' isp, '1' isp_code, '北京'   city_name, 'cn1_01' city_code,'' device, '' ltm, 'cn1' province_code
        | union all
        | select 'pid5' as id, '20191111' as day, '联通' isp, '2' isp_code, '北京'   city_name, 'cn1_01' city_code,'' device, '' ltm, 'cn1' province_code
        | union all
        | select 'pid6' as id, '20191111' as day, '电信' isp, '3' isp_code, '北京'   city_name, 'cn1_01' city_code,'' device, '' ltm, 'cn1' province_code
        | union all
        | select 'pid4' as id, '20191111' as day, '小灵通' isp, '4' isp_code, '北京' city_name, 'cn1_01' city_code,'' device, '' ltm, 'cn1' province_code
        | union all
        | select 'pid2' as id, '20191112' as day, '小灵通' isp, '4' isp_code, '北京' city_name, 'cn1_01' city_code,
        |   '12345678' as device, '20200617' as ltm, 'cn1' province_code
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

    val pidAttrDF = spark.sql(
      s"""
         |select "pid1" id, "电信" carrier, "常州市" city, "cn3" province_code
         |union all
         |select "pid2" id, "联通" carrier, "西安市" city, "cn18" province_code
         |union all
         |select "pid3" id, "联通" carrier,  "西安市" city, "cn18" province_code
         |union all
         |select "pid4" id, "移动" carrier, "上海" city, "cn1" province_code
         |union all
         |select "pid5" id, "移动" carrier,  "上海" city, "cn1" province_code
         |union all
         |select "1111111" id, "移动" carrier, "上海" city, "cn1" province_code
         |union all
         |select "2222222" id, "联通" carrier,  "常州市" city, "cn18" province_code
         |union all
         |select "3333333" id, "联通" carrier,  "西安市" city, "cn18" province_code
       """.stripMargin).toDF("pid_id", "carrier", "city", "province_code")
    insertDF2Table(pidAttrDF, PropUtils.HIVE_TABLE_DIM_PID_ATTRIBUTE_FULL_PAR_SEC, Some("day='20191111', plat=1"))

    val areaDF = spark.sql(
      s"""
         |select stack(4,
         |  '常州', 'cn3_11', 'cn3',
         |  '上海', 'cn1_01', 'cn1',
         |  '西安', 'cn18_06', 'cn18',
         |  '西安', 'cn18_06', 'cn18'
         |) as (city, city_code, province_code)
        """.stripMargin)

    insertDF2Table(areaDF, PropUtils.HIVE_TABLE_MAPPING_AREA_PAR, Some("flag='20191111'"))


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

  test("pidOperatorMapping_test") {
    prepareWarehouse()
    PidOperatorMapping.main(Array(json))

    val res = spark.sql(
      s"""
         |select *
         |from
         |  ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = "hhhhhhhhhh"
       """.stripMargin).cache()

    res.show(false)

    val resCnt = res.count()

//    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW).show(false)

    assertResult(3)(resCnt)

  }


}
