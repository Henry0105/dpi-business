package com.mob.dataengine.engine.core.profile

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.engine.core.jobsparam.ProfileScoreParam
import com.mob.dataengine.engine.core.profilecal.ProfileCalAppInfoV2
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class ProfileCalAppInfoV2Test extends FunSuite with LocalSparkSession {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)
  var jobCommon: JobCommon = _
  var params: Seq[ProfileScoreParam] = _
  val PARAMS_KEY = "params"


  val json = FileUtils.getJson("unittest/profile_cal/profile_cal_app_info_v2.json")

  def prepare(arg: String): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    jobCommon = JsonMethods.parse(arg)
      .transformField {
        case ("rpc_host", x) => ("rpcHost", x)
        case ("rpc_port", x) => ("rpcPort", x)
      }
      .extract[JobCommon]
    println(jobCommon)

    params = (JsonMethods.parse(arg) \ PARAMS_KEY)
      .transformField {
        case ("hdfs_output", x) => ("hdfsOutput", x)
      }
      .extract[Seq[ProfileScoreParam]]
  }


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


    prepareActiveMonthly()
    // spark.table(PropUtils.HIVE_TABLE_APP_ACTIVE_MONTHLY_PENETRANCE_RATIO).show(truncate = false)

//    prepareProfileFull()
    // spark.table(PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL).show(truncate = false)

    prepareAppRankMonthly()

    prepareAppInfo()
    // spark.table(PropUtils.HIVE_TABLE_APPPKG_INFO).show(truncate = false)

    prepareDataOptCache()
    // spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).show(truncate = false)
    prepareDataOptCacheNew()
    // spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW).show(truncate = false)

    prepareTagsMappingView()
    // spark.table(PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING).show(truncate = false)

    prepareAppCateMap()

    prepareAppInfoDaily()
  }

  def prepareActiveMonthly(): Unit = {
    val sql = FileUtils.
      getSqlScript("conf/sql_scripts/rp_tables_create/rp_mobdi_app/app_active_monthly.sql",
        tableName = PropUtils.HIVE_TABLE_APP_ACTIVE_MONTHLY_PENETRANCE_RATIO)
    spark.sql(sql)

    CSVUtils.fromCsvIntoHive("hive_data/rp_mobdi_app/app_active_monthly.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_APP_ACTIVE_MONTHLY_PENETRANCE_RATIO, partition = "month")
  }

  def prepareAppRankMonthly(): Unit = {
    val sql = FileUtils.
      getSqlScript("conf/sql_scripts/rp_tables_create/rp_mobdi_app/rp_app_rank_category_install_monthly.sql",
        tableName = PropUtils.HIVE_TABLE_APP_INSTALL_PENETRANCE_RATIO)
    spark.sql(sql)

    CSVUtils.fromCsvIntoHive("hive_data/rp_mobdi_app/rp_app_rank_category_install_monthly.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_APP_INSTALL_PENETRANCE_RATIO, partition = "rank_date")
  }

  def prepareAppCateMap(): Unit = {
    val sql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_sdk_mapping/app_category_mapping_par.sql",
        tableName = PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR)
    spark.sql(sql)

    CSVUtils.fromCsvIntoHive(s"hive_data/dm_sdk_mapping/app_category_mapping.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR, partition = "version")
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
      tableName = PropUtils.HIVE_TABLE_APPINFO_DAILY_V2)
    spark.sql(sql)
  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobeye_app360 CASCADE")
  }

  test(testName = "profile_cal_app_info_v2_test") {
    ProfileCalAppInfoV2.main(Array(json))
    prepare(json)
    spark.table(PropUtils.HIVE_TABLE_APPINFO_DAILY_V2).show(false)

    import spark.implicits._
    assertResult(0)(spark.table(PropUtils.HIVE_TABLE_APPINFO_DAILY_V2)
      .where($"install_tgi".isNull).count())


    val colList = List("apppkg", "install_app_device_cnt",
      "install_device_cnt", "active_app_device_cnt", "active_device_cnt",
      "install_penetration", "install_tgi", "active_penetration")
    spark.sparkContext.textFile(params.head.output.hdfsOutput)
      .map(rdd => {
        val ar = rdd.split("\t")
        (ar(0), ar(1), ar(2), ar(3), ar(4), ar(5), ar(6), ar(7)
        )
      }).toDF(colList: _*)
      .createOrReplaceTempView("out_csv_df")

    val installAppDeviceCnt = spark.sql(
      s"""
         |select install_app_device_cnt from out_csv_df
         |where apppkg='com.autonavi.minimap'
       """.stripMargin).head()
      .getAs[String]("install_app_device_cnt")
    assert(installAppDeviceCnt == 4.toString)

    val filterCnt = spark.table("out_csv_df")
      .filter($"apppkg" === "com.android.bbk.lockscreen").count()
    assert(filterCnt == 0.toLong)
  }
}
