package com.mob.dataengine.engine.core.profile

import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.engine.core.profilecal.ProfileCalSourceAndFlow
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class ProfileCalSourceAndFlowTest extends FunSuite with LocalSparkSession {
  val json = FileUtils.getJson("unittest/profile_cal/profile_cal_source_flow.json")


  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql("DROP DATABASE IF EXISTS ext_ad_dmp CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_master CASCADE")
    spark.sql("create database dm_dataengine_tags")
    spark.sql("create database rp_dataengine")
    spark.sql("create database rp_mobdi_app")
    spark.sql("create database ext_ad_dmp")
    spark.sql("create database dm_mobdi_master")

    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql("DROP DATABASE IF EXISTS ext_ad_dmp CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_master CASCADE")
  }

  def prepareWarehouse(): Unit = {
    prepareFrequency3Monthly()
    prepareDataOptCache()
    prepareDataOptCacheNew()
    prepareTagsMappingView()
    preparepoiGeohash()
    preparepoiStayingDaily()
    preparepoiSourceAndFlowDaily()
  }

  def prepareFrequency3Monthly(): Unit = {
    val sql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_mobdi_app/rp_device_frequency_3monthly.sql",
        tableName = PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive("hive_data/rp_mobdi_app/rp_device_frequency_3monthly.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY,
      partition = "day")
  }

  def prepareDataOptCache(): Unit = {
    val sql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
        tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive(s"hive_data/rp_dataengine/data_opt_cache.csv",
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
        "conf/sql_scripts/dm_tables_create/dm_dataengine_tags/dm_device_latest_tags_mapping_view.sql",
        tableName = PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive("hive_data/dm_dataengine_tags/dm_device_latest_tags_mapping_view.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING,
      partition = "version")
  }

  def preparepoiGeohash(): Unit = {
    val sql = FileUtils.getSqlScript("conf/sql_scripts/ext_table_create/ext_ad_dmp/dw_base_poi_l1_geohash.sql",
        tableName = PropUtils.HIVE_TABLE_DW_BASE_POI_L1_GEOHASH)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive("hive_data/ext_ad_dmp/dw_base_poi_l1_geohash.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DW_BASE_POI_L1_GEOHASH)
  }

  def preparepoiStayingDaily(): Unit = {
    val sql = FileUtils.getSqlScript(
      "conf/sql_scripts/dm_tables_create/dm_mobdi_master/device_staying_daily.sql",
        tableName = PropUtils.HIVE_TABLE_DEVICE_STAYING_DAILY)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive("hive_data/dm_mobdi_master/device_staying_daily.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DEVICE_STAYING_DAILY, partition = "day")
  }

  def preparepoiSourceAndFlowDaily(): Unit = {
    val sql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_dataengine/mobeye_o2o_lbs_sourceandflow_daily.sql",
        tableName = PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_SOUREANDFLOW_DAILY)
    spark.sql(sql)
  }


  test(testName = "source_flow_test") {
    ProfileCalSourceAndFlow.main(Array(json))
    spark.table(PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_SOUREANDFLOW_DAILY).show(false)
  }
}