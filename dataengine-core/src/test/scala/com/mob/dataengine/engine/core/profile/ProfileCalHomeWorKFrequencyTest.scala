package com.mob.dataengine.engine.core.profile

import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.engine.core.profilecal.ProfileCalHomeWorKFrequency
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class ProfileCalHomeWorKFrequencyTest  extends FunSuite with LocalSparkSession{
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  val json = FileUtils.getJson(s"unittest/profile_cal/profile_cal_frequency.json")

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS ext_ad_dmp CASCADE")
    spark.sql(sqlText = "CREATE DATABASE rp_mobdi_app")
    spark.sql(sqlText = "CREATE DATABASE rp_dataengine")
    spark.sql(sqlText = "CREATE DATABASE dm_dataengine_tags")
    spark.sql(sqlText = "CREATE DATABASE ext_ad_dmp")

    prepareLocation3Monthly()
    prepareFrequency3Monthly()
    prepareDataOptCache()
    prepareDataOptCacheNew()
    prepareTagsMappingView()
    preparepoiGeohash()
    prepareFrequencyDaily()
    prepareHomeAndWorkDaily()
  }

  def prepareLocation3Monthly(): Unit = {
    val sql = FileUtils.
      getSqlScript("conf/sql_scripts/rp_tables_create/rp_mobdi_app/rp_device_location_3monthly.sql",
        tableName = PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive(s"hive_data/rp_mobdi_app/rp_device_location_3monthly.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK,
      partition = "day")
  }

  def prepareFrequency3Monthly(): Unit = {
    val sql = FileUtils.getSqlScript(
        s"conf/sql_scripts/rp_tables_create" +
          s"/rp_mobdi_app/rp_device_frequency_3monthly.sql",
        tableName = PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY
      )
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive(s"hive_data/rp_mobdi_app/rp_device_frequency_3monthly.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY, partition = "day")
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
    CSVUtils.fromCsvIntoHive(s"hive_data/dm_dataengine_tags/dm_device_latest_tags_mapping_view.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING,
      partition = "version")
  }

  def preparepoiGeohash(): Unit = {
    val sql = FileUtils.getSqlScript("conf/sql_scripts/ext_table_create/ext_ad_dmp/dw_base_poi_l1_geohash.sql",
        tableName = PropUtils.HIVE_TABLE_DW_BASE_POI_L1_GEOHASH)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive(s"hive_data/ext_ad_dmp/dw_base_poi_l1_geohash.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DW_BASE_POI_L1_GEOHASH)
  }

  def prepareFrequencyDaily(): Unit = {
    val sql = FileUtils.getSqlScript(
        "conf/sql_scripts/rp_tables_create/rp_dataengine/mobeye_o2o_lbs_frequency_daily.sql",
        tableName = PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_FREQUENCY_DAILY)
    spark.sql(sql)
  }

  def prepareHomeAndWorkDaily(): Unit = {
    val sql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_dataengine/mobeye_o2o_lbs_homeandwork_daily.sql",
        tableName = PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_HOMEANDWORK_DAILY)
    spark.sql(sql)
  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS ext_ad_dmp CASCADE")
  }

  /* test(testName = "profileCalHomeWorkFrequencyTest") {
    val profileCalTaskJob = ProfileCalTaskJob(json)

    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName(profileCalTaskJob.jobId)
      .enableHiveSupport()
      .getOrCreate()

    ProfileHomeWorkFrequency.calO2OHomeWorkFrequency(spark, profileCalTaskJob)

    spark.table(PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_FREQUENCY_DAILY).show(false)
    spark.table(PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_HOMEANDWORK_DAILY).show(false)

    // TODO 对result进行check
  } */

  test(testName = "profileCalHomeWorkFrequencyTest") {
    ProfileCalHomeWorKFrequency.main(Array(json))
    spark.table(PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_HOMEANDWORK_DAILY)
      .show(false)
  }
}
