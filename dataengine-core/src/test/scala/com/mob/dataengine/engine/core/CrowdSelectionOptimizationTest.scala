package com.mob.dataengine.engine.core

import java.util.Date

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.crowd.CrowdSelectionOptimization
import com.mob.dataengine.utils.FileUtils
import com.mob.dataengine.engine.core.jobsparam.CrowdSelectionOptimizationParam
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.{DataFrame, LocalSparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.scalatest.FunSuite

class CrowdSelectionOptimizationTest extends FunSuite with LocalSparkSession {
  var finalDF: DataFrame = _
  var jobCommon: JobCommon = _
  var params: Seq[CrowdSelectionOptimizationParam] = _
  val PARAMS_KEY = "params"

  val jsonNone = FileUtils.getJson("unittest/crowd_set_operation/crowd_selection_optimization_none.json")
  val jsonApppkg = FileUtils.getJson("unittest/crowd_set_operation/crowd_selection_optimization_apppkg.json")
  val jsonCate2 = FileUtils.getJson("unittest/crowd_set_operation/crowd_selection_optimization_cate_l2.json")
  val jsonCate1 = FileUtils.getJson("unittest/crowd_set_operation/crowd_selection_optimization_cate_l1.json")

  def prepare(arg: String): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    jobCommon = JsonMethods.parse(arg)
      .transformField {
        case ("job_id", x) => ("jobId", x)
        case ("job_name", x) => ("jobName", x)
        case ("rpc_host", x) => ("rpcHost", x)
        case ("rpc_port", x) => ("rpcPort", x)
      }
      .extract[JobCommon]

    params = (JsonMethods.parse(arg) \ PARAMS_KEY)
      .transformField {
        case ("id_type", x) => ("idType", x)
        case ("id_types", x) => ("idTypes", x)
        case ("hdfs_output", x) => ("hdfsOutput", x)
        case ("encrypt_type", x) => ("encryptType", x)
        case ("data_process", x) => ("dataProcess", x)
        case ("profile_ids", x) => ("profileIds", x)
        case ("min_day", x) => ("minDay", x)
        case ("approximation_day", x) => ("approximationDay", x)
      }
      .extract[Seq[CrowdSelectionOptimizationParam]]
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_sdk_mapping")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS " +
      s"${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL.split("\\.")(0)} CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE ${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL.split("\\.")(0)}")


    // 创建存储结果表
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)} CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)}")

    Seq(
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf0", "20190401", "cf|4", "9c8db41e4073efa67a3da8d1915e5c33"),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c", "20190401", "cf|4", "9c8db41e4073efa67a3da8d1915e5c33"),
      ("04399d01c2e2fac798f4f1b47285185ebbdca738", "20190401", "cf|4", "9c8db41e4073efa67a3da8d1915e5c33")
    ).toDF(
      "data", "created_day", "biz", "uuid"
    ).write.partitionBy("created_day", "biz", "uuid")
      .saveAsTable(PropUtils.HIVE_TABLE_DATA_OPT_CACHE)


    val mobeyeO2OLogCleanSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/mobeye_o2o_log_clean.sql",
      tableName = PropUtils.HIVE_TABLE_MOBEYE_O2O_LOG_CLEAN)
    spark.sql(
      s"""
         |$mobeyeO2OLogCleanSql
       """.stripMargin)

    val deviceProfileFullSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/rp_device_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL)
    spark.sql(
      s"""
         |$deviceProfileFullSql
       """.stripMargin)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL}
         |  select
         |  "cdf7464d664816d83adb983ff45bc4e6bbc70bf0" AS device,
         |  null as carrier,
         |  null as network,
         |  null as cell_factory,
         |  null as sysver,
         |  null as model,
         |  null as model_level,
         |  null as screensize,
         |  null as country,
         |  null as province,
         |  null as city,
         |  null as city_level,
         |  null as country_cn,
         |  null as province_cn,
         |  null as city_cn,
         |  null as breaked,
         |  null as city_level_1001,
         |  null as public_date,
         |  null as identity,
         |  null as gender,
         |  null as agebin,
         |  null as car,
         |  null as married,
         |  null as edu,
         |  null as income,
         |  null as house,
         |  null as kids,
         |  null as occupation,
         |  null as industry,
         |  null as life_stage,
         |  null as special_time,
         |  null as consum_level,
         |  null as agebin_1001,
         |  null as tag_list,
         |  null as repayment,
         |  null as segment,
         |  null as applist,
         |  null as tot_install_apps,
         |  null as nationality,
         |  null as nationality_cn,
         |  null as last_active,
         |  null as group_list,
         |  null as first_active_time,
         |  null as catelist,
         |  null as permanent_country,
         |  null as permanent_province,
         |  null as permanent_city,
         |  null as permanent_country_cn,
         |  null as permanent_province_cn,
         |  null as permanent_city_cn,
         |  null as workplace,
         |  null as residence,
         |  null as mapping_flag,
         |  null as model_flag,
         |  null as stats_flag,
         |  null as processtime,
         |  null as processtime_all,
         |  null as price,
         |  null as permanent_city_level
       """.stripMargin)


    val _halfYearBefore = DateFormatUtils.format(
      org.apache.commons.lang3.time.DateUtils.addMonths(new Date(), -1), "yyyyMMdd")
    Seq(
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf0", 1, "com.tencent.mm",
        "com.tencent.mm", _halfYearBefore, _halfYearBefore),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c", 1, "",
        "", _halfYearBefore, _halfYearBefore),
      ("04399d01c2e2fac798f4f1b47285185ebbdca738", 1, "com.iqiyi.mm",
        "com.iqiyi.mm", _halfYearBefore, _halfYearBefore)
    ).toDF(
      "device", "plat", "pkg", "apppkg", "processtime", "day"
    ).write.partitionBy("day")
      .saveAsTable(PropUtils.HIVE_TABLE_DEVICE_ACTIVE_APPLIST_FULL)

    Seq(
      ("com.tencent.mm", "com.tencent.mm", "包名1", "一级1", "二级1", "c1", "c1_1", "20190408"),
      ("com.iqiyi.mm", "com.iqiyi.mm", "包名2", "一级2", "二级1", "c2", "c2_1", "20190408")
    ).toDF(
      "pkg", "apppkg", "appname", "cate_l1", "cate_l2", "cate_l1_id", "cate_l2_id", "version"
    ).write.partitionBy("version")
      .saveAsTable(PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR)
  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS " +
      s"${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL.split("\\.")(0)} CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)} CASCADE")

//    stop()
  }


  test(testName = "crowd selection optimization || NONE") {
    prepare(jsonNone)
    CrowdSelectionOptimization.main(Array(jsonNone))
    finalDF = spark.sql(
      s"""
         |SELECT data FROM ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |WHERE created_day=${jobCommon.day}
         |AND biz='${JobName.getId(jobCommon.jobName)}|${DeviceType.DEVICE.id}'
         |AND uuid='${params.head.output.uuid}'
       """.stripMargin
    )
    val resCnt = finalDF.count().toInt
    assert(resCnt == 1)
  }

  test(testName = "crowd selection optimization || APPPKG") {
    prepare(jsonApppkg)
    CrowdSelectionOptimization.main(Array(jsonApppkg))
    finalDF = spark.sql(
      s"""
         |SELECT data FROM ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |WHERE created_day=${jobCommon.day}
         |AND biz='${JobName.getId(jobCommon.jobName)}|${DeviceType.DEVICE.id}'
         |AND uuid='${params.head.output.uuid}'
       """.stripMargin
    )
    val resCnt = finalDF.count().toInt
    assert(resCnt == 2)
  }

  test(testName = "crowd selection optimization || CATEL1") {
    prepare(jsonCate1)
    CrowdSelectionOptimization.main(Array(jsonCate1))
    finalDF = spark.sql(
      s"""
         |SELECT data FROM ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |WHERE created_day=${jobCommon.day}
         |AND biz='${JobName.getId(jobCommon.jobName)}|${DeviceType.DEVICE.id}'
         |AND uuid='${params.head.output.uuid}'
       """.stripMargin
    )
    val resCnt = finalDF.count().toInt
    assert(resCnt == 2)
  }

  test(testName = "crowd selection optimization || CATEL2") {
    prepare(jsonCate2)
    CrowdSelectionOptimization.main(Array(jsonCate2))
    finalDF = spark.sql(
      s"""
         |SELECT data FROM ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |WHERE created_day=${jobCommon.day}
         |AND biz='${JobName.getId(jobCommon.jobName)}|${DeviceType.DEVICE.id}'
         |AND uuid='${params.head.output.uuid}'
       """.stripMargin
    )
    val resCnt = finalDF.count().toInt
    assert(resCnt == 2)
  }

}
