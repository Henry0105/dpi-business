package com.mob.dataengine.engine.core.profile

import com.mob.dataengine.commons.utils.CSVUtils
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.profilecal.ProfileCalAppInfo
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class ProfileCalAppInfoTest extends FunSuite with LocalSparkSession {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  val json = FileUtils.getJson("unittest/profile_cal/profile_cal_app_info.json")


  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobeye_app360 CASCADE")
    spark.sql(sqlText = "CREATE DATABASE rp_dataengine")
    spark.sql(sqlText = "CREATE DATABASE dm_mobdi_mapping")
    spark.sql(sqlText = "CREATE DATABASE rp_mobdi_app")
    spark.sql(sqlText = "CREATE DATABASE dm_sdk_mapping")
    spark.sql(sqlText = "CREATE DATABASE dm_dataengine_tags")
    spark.sql(sqlText = "CREATE DATABASE rp_mobeye_app360")


    prepareActiveWeekly()
    // spark.table(PropUtils.HIVE_TABLE_APP_ACTIVE_WEEKLY_PENETRANCE_RATIO).show(truncate = false)

    prepareAppRankMonthly()

    prepareProfileFull()
    // spark.table(PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL).show(truncate = false)

    prepareAppInfo()
    // spark.table(PropUtils.HIVE_TABLE_APPPKG_INFO).show(truncate = false)

    prepareDataOptCache()
    // spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).show(truncate = false)
    prepareDataOptCacheNew()
    // spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW).show(truncate = false)

    prepareTagsMappingView()
    // spark.table(PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING).show(truncate = false)

    prepareAppInfoDaily()
  }

  def prepareActiveWeekly(): Unit = {
    val sql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_mobdi_mapping/app_active_weekly_penetrance_ratio.sql",
        tableName = PropUtils.HIVE_TABLE_APP_ACTIVE_WEEKLY_PENETRANCE_RATIO)
    spark.sql(sql)

    CSVUtils.fromCsvIntoHive("hive_data/dm_mobdi_mapping/app_active_weekly_penetrance_ratio.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_APP_ACTIVE_WEEKLY_PENETRANCE_RATIO, partition = "rank_date")
  }

  def prepareAppRankMonthly(): Unit = {
    val sql = FileUtils.
      getSqlScript("conf/sql_scripts/rp_tables_create/rp_mobdi_app/rp_app_rank_category_install_monthly.sql",
        tableName = PropUtils.HIVE_TABLE_APP_INSTALL_PENETRANCE_RATIO)
    spark.sql(sql)

    CSVUtils.fromCsvIntoHive("hive_data/rp_mobdi_app/rp_app_rank_category_install_monthly.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_APP_INSTALL_PENETRANCE_RATIO, partition = "rank_date")
  }


  def prepareProfileFull(): Unit = {

    val sql = FileUtils.
      getSqlScript("conf/sql_scripts/rp_tables_create/rp_mobdi_app/rp_device_profile_full_view.sql",
        tableName = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL)
    spark.sql(sql)

    CSVUtils.fromCsvIntoHive(s"hive_data/rp_mobdi_app/rp_device_profile_full_view.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL, partition = "version")
  }

  def prepareAppInfo(): Unit = {
    val sql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_sdk_mapping/apppkg_info.sql",
        tableName = PropUtils.HIVE_TABLE_APPPKG_INFO)
    spark.sql(sql)

    CSVUtils.fromCsvIntoHive("hive_data/dm_sdk_mapping/apppkg_info.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_APPPKG_INFO)
  }

  def prepareDataOptCache(): Unit = {
    val sql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive("hive_data/rp_dataengine/data_opt_cache.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      partition = "created_day", "biz", "uuid")
  }

  def prepareDataOptCacheNew(): Unit = {
    val sql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    spark.sql(sql)
  }

  def prepareTagsMappingView(): Unit = {
    val sql = FileUtils.getSqlScript(
      s"conf/sql_scripts/dm_tables_create/dm_dataengine_tags/dm_device_latest_tags_mapping_view.sql",
      tableName = PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive("hive_data/dm_dataengine_tags/dm_device_latest_tags_mapping_view.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING, partition = "version")
  }

  def prepareAppInfoDaily(): Unit = {
    val sql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/rp_dataengine/appinfo_daily.sql",
      tableName = PropUtils.HIVE_TABLE_APPINFO_DAILY)
    spark.sql(sql)
  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
  }

  test(testName = "profile_call_app_info_test") {
    ProfileCalAppInfo.main(Array(json))
    spark.table(PropUtils.HIVE_TABLE_APPINFO_DAILY).show(false)
    // TODO 对result进行check
    import spark.implicits._
    val indexRes = spark.table(PropUtils.HIVE_TABLE_APPINFO_DAILY).
      where($"apppkg" === "com.android.bbk.lockscreen").select("index").head().getDouble(0)
    assertResult(142.85714285714283)(indexRes)

    assertResult(0)(spark.table(PropUtils.HIVE_TABLE_APPINFO_DAILY).
      where($"apppkg" === "air.tv.douyu.android1").count())

  }
}