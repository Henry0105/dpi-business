package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.utils.CSVUtils
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.portrait.CrowdPortraitCalculation
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest._

/**
 * @author juntao zhang
 */
class CrowdPortraitCalculationTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
    spark.sql("create database dm_dataengine_tags")
    spark.sql("create database rp_dataengine")
    spark.sql(sqlText = "CREATE DATABASE rp_mobdi_app")
    spark.sql(sqlText = "CREATE DATABASE dm_mobdi_mapping")
    val dataOPTCacheNewSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql", tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val sql = FileUtils.getSqlScript(
        s"conf/sql_scripts/rp_tables_create/rp_mobdi_app/rp_device_profile_full_view.sql",
        tableName = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL)
    createTable(sql)

    val scoresql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_dataengine/crowd_portrait_calculation_score.sql",
        tableName = PropUtils.HIVE_TABLE_CROWD_PORTRAIT_CALCULATION_SCORE)
    createTable(scoresql)

    val weeklySql = FileUtils.getSqlScript(
        s"conf/sql_scripts/rp_tables_create/rp_mobdi_app/app_active_weekly.sql",
        tableName = PropUtils.HIVE_TABLE_APP_ACTIVE_WEEKLY)
    createTable(weeklySql)

    val outingSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/rp_mobdi_app/rp_device_outing.sql",
        tableName = PropUtils.HIVE_TABLE_RP_DEVICE_OUTING)
    createTable(outingSql)

    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
  }

  def prepareWarehouse(): Unit = {
    Seq(
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf0", "20190401", "cf|4", "275662018884415488"),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c", "20190401", "cf|4", "275662018884415488"),
      ("04399d01c2e2fac798f4f1b47285185ebbdca738", "20190401", "cf|4", "275662018884415488"),
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf02", "20190401", "cf|4", "2756620188844154882"),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c2", "20190401", "cf|4", "2756620188844154882"),
      ("04399d01c2e2fac798f4f1b47285185ebbdca7382", "20190401", "cf|4", "2756620188844154882")
    ).toDF(
      "data", "created_day", "biz", "uuid"
    ).write.partitionBy("created_day", "biz", "uuid")
      .saveAsTable(PropUtils.HIVE_TABLE_DATA_OPT_CACHE)

    CSVUtils.fromCsvIntoHive("hive_data/rp_mobdi_app/rp_device_profile_full_view.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL, partition = "version")

    CSVUtils.fromCsvIntoHive("hive_data/rp_mobdi_app/app_active_weekly.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_APP_ACTIVE_WEEKLY, partition = "par_time")

    CSVUtils.fromCsvIntoHive("hive_data/rp_mobdi_app/rp_device_outing.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_RP_DEVICE_OUTING, "day", "time_window")
  }

  test("crowd calculation test") {
    val myJSON = FileUtils.getJson("unittest/crowd_portrait/crowd_portrait_calculation1_new.json")
    CrowdPortraitCalculation.jobNums = 15
    CrowdPortraitCalculation.main(Array(myJSON))
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    println(CrowdPortraitCalculation.jobContext.jobCommon.toString)
    println(CrowdPortraitCalculation.jobContext.params.head.inputs.head.toString)
    println(CrowdPortraitCalculation.jobContext.params.head.output.toString)
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

    val deviceCnt = spark.sql(
      s"""
         |select * from
         |${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '11328a45f49e6c545b715a1ad2831af0'
      """.stripMargin).count()
    assert(deviceCnt > 0)
    val cnt = spark.sql(
      s"""
        |select * from
        |${PropUtils.HIVE_TABLE_CROWD_PORTRAIT_CALCULATION_SCORE}
        |where uuid = '11328a45f49e6c545b715a1ad2831af0'
      """.stripMargin).count()
    assert(cnt > 0)
  }

}
