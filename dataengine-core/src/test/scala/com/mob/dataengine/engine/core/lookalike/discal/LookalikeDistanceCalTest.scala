package com.mob.dataengine.engine.core.lookalike.discal

import org.apache.spark.sql.{DataFrame, LocalSparkSession}
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.utils.FileUtils

/**
 * @author liyi
 */
class LookalikeDistanceCalTest extends FunSuite with LocalSparkSession{

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  val json = FileUtils.getJson(s"unittest/lookalike/lookalike_normal.json")

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "CREATE DATABASE rp_dataengine")

    val dataOPTCacheSql = FileUtils.
      getSqlScript(s"conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
        tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE
      )
    println(s"now print single profile info $dataOPTCacheSql")
    spark.sql(
      s"""
         |$dataOPTCacheSql
       """.stripMargin
    )

    val pcaTagsMappingSql =
      FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create" +
      s"/rp_dataengine/rp_mobeye_tfidf_pca_tags_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING)

    spark.sql(pcaTagsMappingSql)

    val pcaDemoMappingSql =
      FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create" +
        s"/rp_dataengine/rp_mobeye_tfidf_pca_demo.sql",
        tableName = PropUtils.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_DEMO)

    spark.sql(pcaDemoMappingSql)

    preparePcaTagsMapping()
    preparePcaDemo()
  }

  def preparePcaTagsMapping(): Unit = {
    CSVUtils.fromCsvIntoHive("hive_data/rp_dataengine/rp_mobeye_tfidf_pca_tags_mapping.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING, partition = "day")
  }

  def preparePcaDemo(): Unit = {
    CSVUtils.fromCsvIntoHive("hive_data/rp_dataengine/rp_mobeye_tfidf_pca_demo.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_DEMO)
  }

  def makeSourceDF1(uuid: String): DataFrame = {
    import spark.implicits._
    Seq(
      "d7e65634dfd8cdaf51bc679b60634dad337b5d04",
      "d83ee321a49a9138b2b3c263070e7d646b5dd6f3",
      "d89632e96ec098005451bee286755022621e4048",
      "d8a701732a666d758f5a20320397a1c4175bd241",
      "d8c949549a8ebe7f98a636269483b8bdbc25da79",
      "d9a91318c9a2a88fa80711f297cf01bdda58a662",
      "da78039af7ddb06d4440ffc28cb657e2e1189165",
      "dae83d6d87f991dd9d44974085f274daa33b3b22",
      "db137e025d5f4e5f15f5e2ce4fdb7eb81d129a05",
      "db14dfa60a1285f0280708ba6b52d1fa29d6bf75"
    ).toDF("device").createOrReplaceTempView("t1")
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |PARTITION(created_day=null,biz=null,uuid='$uuid')
         |SELECT device as data
         |FROM t1
       """.stripMargin
    )
  }

  def makeSourceDF2(uuid: String): DataFrame = {
    import spark.implicits._
    Seq(
      "358761084105381",
      "861257034172308",
      "866518030051415",
      "862299036358427",
      "869026024130655",
      "863098034922260",
      "869938024697592",
      "990005886081760",
      "359881062535554",
      "99001020319767"
    ).toDF("device").createOrReplaceTempView("t2")
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |PARTITION(created_day=null,biz=null,uuid='$uuid')
         |SELECT device as data
         |FROM t2
       """.stripMargin
    )
  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
  }

  test(testName = "normal") {
    makeSourceDF1("1052c9e8b956e41d42e94469732580b2")
    makeSourceDF1("11328a45f49e6c545b715a1ad2831af0")

    LookalikeDistanceCalLaunch.main(Array(json))
    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).where("uuid='uuid_value'").show(false)
  }

  test(testName = "MDML-460_bugfix") {
    makeSourceDF2("1052c9e8b956e41d42e94469732580b2")
    makeSourceDF2("11328a45f49e6c545b715a1ad2831af0")


    LookalikeDistanceCalLaunch.main(Array(json))
    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).where("uuid='uuid_value'").show(false)
  }
}