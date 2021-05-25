package com.mob.dataengine.engine.core.business.dpi

import com.mob.dataengine.commons.{DPIJobCommon, JobCommon}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.business.dpi.been.DPIParam
import com.mob.dataengine.engine.core.jobsparam.{JobContext2, JobParamTransForm}
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.json4s.{DefaultFormats, _}
import org.scalatest.FunSuite

class DpiMktUrlTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  val version = "v3"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dpi_mapping_test CASCADE")

    spark.sql("create database dm_dpi_mapping_test")
    spark.sql("create database dm_dataengine_test")

    val codeMappingSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_code_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
    println(s"now print code mapping info $codeMappingSql")
    createTable(codeMappingSql)

    val dpiMktUrlRecordSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_INPUT)
    createTable(dpiMktUrlRecordSql)

    val domainDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dim_dpi_domain.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_DIM_DOMAIN)
    createTable(domainDF)

    // gen_tag
    val urlTagDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG)
    createTable(urlTagDF)

    val tagInitDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MKT_TAG_INIT)
    createTable(tagInitDF)

    val urlWithtagDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG)
    createTable(urlWithtagDF)

    // gen_tag
    val urlTagHzDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG_HZ)
    createTable(urlTagHzDF)

    val urlWithtagHzDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG_HZ)
    createTable(urlWithtagHzDF)

    // pre_screen
    val preScreenDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_PRE_SCREEN)
    createTable(preScreenDF)

    // mp
    val mpDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_MP)
    createTable(mpDF)

    // res
    val marketPlusDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MARKETPLUS_TAG_URL_MAPPING)
    createTable(marketPlusDF)

    val finDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_FIN_TAG_URL_MAPPING)
    createTable(finDF)

    val mobeyeDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_MOBEYE_TAG_URL_MAPPING)
    createTable(mobeyeDF)

    val gaDF = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dpi_mapping/" +
      "dpi_mkt_url.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DPI_GA_TAG_URL_MAPPING)
    createTable(gaDF)

    prepareMapping()
  }

  def prepareMapping(): Unit = {
    // code mapping
    println("codeDF ==>")
    val codeDF = spark.read.option("header", "true")
      .csv("src/test/resources/dm_dataengine_code_mapping.csv")

    insertDF2Table(codeDF, PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING, Some("day='20200502'"))

    // dim_dpi_domain.csv
    val domainDF = spark.read.option("header", "true")
      .csv("src/test/resources/business/dpi/dim_dpi_domain.csv")
    insertDF2Table(domainDF, PropUtils.HIVE_TABLE_RP_DPI_DIM_DOMAIN)

    // gen_tag
    val tagInitDF = spark.read.option("header", "true")
      .csv("src/test/resources/business/dpi/tag_init.csv")
    insertDF2Table(tagInitDF, PropUtils.HIVE_TABLE_RP_DPI_MKT_TAG_INIT)

    val A9 = ("'A'," * 9).init
    val B9 = ("'B'," * 9).init
    val C9 = ("'C'," * 9).init
    val D9 = ("'D'," * 9).init
    sql(
      s"""
         |INSERT INTO TABLE ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG}
         |PARTITION(carrier = 'unicom', version = 'v1')
         |VALUES ("800", $A9)
         |""".stripMargin)
    sql(
      s"""
         |INSERT INTO TABLE ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG}
         |PARTITION(carrier = 'unicom', version = 'v2')
         |VALUES ("801", $B9)
         |""".stripMargin)
    sql(
      s"""
         |INSERT INTO TABLE ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG_HZ}
         |PARTITION(version = 'v1')
         |VALUES ("802", $C9)
         |""".stripMargin)

    sql(
      s"""
         |INSERT INTO TABLE ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG_HZ}
         |PARTITION(version = 'v2')
         |VALUES ("803", $D9)
         |""".stripMargin)

  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dpi_mapping_test CASCADE")
  }

  test("基本流程测试") {
    import org.apache.spark.sql.functions._
    val js =
      s"""
         |{
         |  "jobId": "business_dpi_mkt_url_demo",
         |  "jobName": "business_dpi_mkt_url",
         |  "rpcHost": null,
         |  "userId": "145",
         |  "rpcPort": 0,
         |  "day": "20201201",
         |  "params": [
         |    {
         |      "inputs": [{
         |          "value":"src/test/resources/business/dpi/1123_game_zj.csv",
         |          "compressType":"",
         |          "sep" : ",",
         |          "uuid": "$version",
         |          "url":"src/test/resources/business/dpi/1123_game_zj.csv",
         |          "carriers": ["unicom","henan_mobile","shandong_mobile","hebei_mobile","anhui_mobile","jiangsu_mobile","tianjin_mobile","zhejiang_mobile","telecom"],
         |          "business": ["2"]
         |       }]
         |    }
         |  ]
         |}
       """.stripMargin
    // "henan_mobile"

    val PARAMS_KEY = "params"
    implicit val formats: DefaultFormats.type = DefaultFormats
    val jobCommon = JobParamTransForm.humpConversion(js)
      .extract[DPIJobCommon]
    val params = (JobParamTransForm.humpConversion(js) \ PARAMS_KEY)
      .extract[Seq[DPIParam]]

    params.foreach { param =>
      DpiMktUrl.jobContext = JobContext2(spark, jobCommon, param)
      DpiMktUrl.run(DpiMktUrl.jobContext)
    }

    println("结果数据测试 ==>")
    spark.table(PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG)
      .filter($"version" === s"$version").show(false)
    spark.table(PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG_HZ)
      .filter($"version" === s"$version").show(false)
    spark.table(PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_PRE_SCREEN)
      .where($"version" === s"$version").show(false)
    spark.table(PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_MP)
      .where($"version" === s"$version").show(false)
    spark.table(PropUtils.HIVE_TABLE_RP_DPI_MARKETPLUS_TAG_URL_MAPPING)
      .where($"version" === s"$version").show(false)
    spark.table(PropUtils.HIVE_TABLE_RP_DPI_FIN_TAG_URL_MAPPING)
      .where($"version" === s"$version").show(false)

  }

}
