package com.mob.dataengine.engine.core.profile


import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class ProfileCalScoreTest extends FunSuite with LocalSparkSession{

  val json = FileUtils.getJson("unittest/profile_cal/profile_cal_score.json")

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = "CREATE DATABASE rp_dataengine")
    spark.sql(sqlText = "CREATE DATABASE dm_dataengine_tags")
    prepareDataOptCache()
    prepareDataOptCacheNew()
    prepareTagsMappingView()
    prepareBaseScoreDaily()
  }

  def prepareDataOptCache(): Unit = {
    val sql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
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

    val sql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create" +
          s"/dm_dataengine_tags/dm_device_latest_tags_mapping_view.sql",
        tableName = PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive(s"hive_data/dm_dataengine_tags/dm_device_latest_tags_mapping_view.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING,
      partition = "version")
  }

  def prepareBaseScoreDaily(): Unit = {
    val sql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/rp_dataengine/mobeye_o2o_base_score.sql",
        tableName = PropUtils.HIVE_TABLE_MOBEYE_O2O_BASE_SCORE_DAILY)
    spark.sql(sql)
  }


  override def afterAll(): Unit = {
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
  }

  test(testName = "profile_cal_score_unitest") {
    ProfileCalScore.main(Array(json))
    spark.table(PropUtils.HIVE_TABLE_MOBEYE_O2O_BASE_SCORE_DAILY).show(false)
  }
}
