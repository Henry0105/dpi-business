package com.mob.dataengine.engine.core

import java.io.File
import java.util.Date

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.CrowdPortraitEstimationParam
import com.mob.dataengine.engine.core.portrait.CrowdPortraitEstimation
import com.mob.dataengine.utils.FileUtils
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.{DataFrame, LocalSparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.scalatest.FunSuite

class CrowdPortraitEstimationTest extends FunSuite with LocalSparkSession {
  var finalDF: DataFrame = _
  var jobCommon: JobCommon = _
  var params: Seq[CrowdPortraitEstimationParam] = _
  val PARAMS_KEY = "params"

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
      }
      .extract[Seq[CrowdPortraitEstimationParam]]
  }

  private val jsonNone = FileUtils.getJson("unittest/crowd_portrait/crowd_portrait_estimation.json")

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_sdk_mapping")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS " +
      s"${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL.split("\\.")(0)} CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE ${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL.split("\\.")(0)}")


    // 创建存储结果表
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)} CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)}")

    val dataOPTCacheSql = FileUtils.
      getSqlScript("conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
        tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE
      )
    println(s"now print single profile info $dataOPTCacheSql")
    createTable(dataOPTCacheSql)

    val optDF = Seq(
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf0", "20190401", "cf|4", "d86e610a1f856ce3303e8ad7b7e54d9b"),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c", "20190401", "cf|4", "d86e610a1f856ce3303e8ad7b7e54d9b"),
      ("04399d01c2e2fac798f4f1b47285185ebbdca738", "20190401", "cf|4", "d86e610a1f856ce3303e8ad7b7e54d9b")
    ).toDF(
      "data", "created_day", "biz", "uuid"
    )
    insertDF2Table(optDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day, biz, uuid"), false)


    val dataOPTCacheNewSql = FileUtils.
      getSqlScript(
        s"conf/sql_scripts/rp_tables_create" +
          s"/rp_dataengine/data_opt_cache.sql",
        tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW
      )
    spark.sql(
      s"""
         |$dataOPTCacheNewSql
       """.stripMargin
    )

    val sql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create" +
          s"/rp_mobdi_app/rp_device_profile_full_view.sql",
        tableName = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL)
    createTable(sql)

    val portraitSourceDeviceSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/crowd_portrait_estimation.sql",
      tableName = PropUtils.HIVE_TABLE_CROWD_PORTRAIT_SOURCE_DEVCIE_PROFILE)
    createTable(portraitSourceDeviceSql)

    val crowdPortraitEstimationSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/crowd_portrait_estimation.sql",
      tableName = PropUtils.HIVE_TABLE_CROWD_PORTRAIT_ESTIMATION_SCORE)
    createTable(crowdPortraitEstimationSql)


    Seq(
      (1, 1, 1, 0.36, 3, 0.2, 2, 0.28, "价格范围|低"),
      (4, 1, 2, 0.5, 1, 0.1, 0, 0.46, "价格范围|中"),
      (6, 1, 2, 0.5, 2, 0.1, 6, 0.45, "价格范围|中")
    ).toDF(
      "id", "type", "sub_type", "basis", "property_type", "property_coefficient", "property_code",
      "property_code_coefficient", "desc"
    ).write.saveAsTable(PropUtils.HIVE_TABLE_HOME_IMPROVEMENT_COEFFICIENT)

    Seq(
      (101, 7, "罗莱家纺", 18.71925, 37.4385, "", "家居建材", "家居家纺", "", 799.0, "高", 5),
      (102, 7, "水星家纺", 13.36545, 26.7309, "", "家居建材", "家居家纺", "", 399.0, "中", 3),
      (103, 7, "富安娜", 11.63635, 23.2727, "", "家居建材", "家居家纺", "", 699.0, "中高", 4),
      (104, 7, "多喜爱", 9.6038, 19.2076, "", "家居建材", "家居家纺", "", 299.0, "中低", 2)
    ).toDF("id", "type", "brand", "min", "max", "desc", "cat1", "cat2", "cat3",
      "price", "price_level", "price_level_code")
      .write.saveAsTable(PropUtils.HIVE_TABLE_HOME_IMPROVEMENT_BRAND)

    Seq(
      (6, 0, 3, "服装", 0.6571)
    ).toDF("agebin", "gender", "occupation", "cate", "weight")
      .write.saveAsTable(PropUtils.HIVE_TABLE_CLOTHING_MEGA_CATE_MAPPING)

    Seq(
      (6, 0, 3, 2, "皮具箱包", "皮具箱包", "旅行箱包", "平价", 0.0149)
    ).toDF("agebin", "gender", "occupation", "income", "cate1", "cate2", "cate3", "price", "weight")
      .write.saveAsTable(PropUtils.HIVE_TABLE_CLOTHING_CATE_MAPPING)

    Seq(
      ("上海", "皮具箱包", "旅行箱包", "2", 2, "上海", "cn25_01", "cn25", "new feeling新感觉", 1, 0.056818181818181816),
      ("北京", "皮具箱包", "旅行箱包", "2", 2, "北京", "cn25_01", "cn25", "37°Love", 1, 0.056818181818181816),
      ("南京", "皮具箱包", "旅行箱包", "3", 2, "江苏", "cn25_01", "cn25", "伊丝艾拉", 3, 0.17045454545454544)
    ).toDF("city", "cat2", "cat3", "price_level", "level", "province_cn", "city_code",
      "province_code", "nameb", "cnt", "percent")
      .write.saveAsTable(PropUtils.HIVE_TABLE_CITY_CATE_CLOTHING_BRAND_MAPPING)

    Seq(
      ("皮具箱包", "旅行箱包", "2", 1, "上海", "cn1", "Farmanl法曼儿", 10, 0.16666666666666666),
      ("皮具箱包", "旅行箱包", "2", 1, "上海", "cn1", "Invaria意娃娜", 1, 0.016666666666666666),
      ("皮具箱包", "旅行箱包", "3", 1, "上海", "cn1", "Just For You", 8, 0.13333333333333333),
      ("皮具箱包", "旅行箱包", "3", 1, "上海", "cn1", "LA CLOVER兰卡文", 3, 0.05)
    ).toDF("cat2", "cat3", "price_level", "level", "province_cn", "province_code", "nameb", "cnt", "percent")
      .write.saveAsTable(PropUtils.HIVE_TABLE_CITY_LEVEL_CATE_CLOTHING_BRAND_MAPPING)

    Seq(
      ("北京", 1),
      ("深圳", 1),
      ("天津", 2),
      ("杭州", 2),
      ("南京", 2),
      ("上海", 1)
    ).toDF("city", "level")
      .write.saveAsTable(PropUtils.HIVE_TABLE_CITY_LEVEL_MAPPING)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL} PARTITION(version)
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
         |  '北京' as province_cn,
         |  '北京' as city_cn,
         |  null as breaked,
         |  null as city_level_1001,
         |  null as public_date,
         |  null as identity,
         |  0 as gender,
         |  6 as agebin,
         |  null as car,
         |  null as married,
         |  null as edu,
         |  2 as income,
         |  null as house,
         |  null as kids,
         |  3 as occupation,
         |  3 as industry,
         |  null as life_stage,
         |  null as special_time,
         |  null as consum_level,
         |  null as agebin_1001,
         |  null as tag_list,
         |  null as repayment,
         |  null as segment,
         |  'com.tencent.mm,com.iqiyi.mm' as applist,
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
         |  null as permanent_city_level,
         |  '20190515.1000' as version
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

    val sqlAppCateMappingPar = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_sdk_mapping/app_category_mapping_par.sql",
        tableName = PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR)
    spark.sql(sqlAppCateMappingPar)

    val appCateDF = spark.sql(
      s"""
         |select "com.tencent.mm" pkg, "com.tencent.mm" apppkg, "包名1" appname,
         |  "一级1" cate_l1, "在线购物" cate_l2, "c1" cate_l1_id, "c1_1" cate_l2_id
         |union all
         |select "com.iqiyi.mm" pkg, "com.iqiyi.mm" apppkg, "包名2" appname,
         |  "一级2" cate_l1, "跨境电商" cate_l2, "c2" cate_l1_id, "c2_1" cate_l2_id
       """.stripMargin)
    insertDF2Table(appCateDF, PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR, Some("version=1000"))

  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS " +
      s"${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL.split("\\.")(0)} CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)} CASCADE")

//    stop()
  }

  test(testName = "crowd portrait estimation") {
    prepare(jsonNone)
    CrowdPortraitEstimation.main(Array(jsonNone))

    val portraitSourceDF = spark.sql(
      s"""
         |select * from ${PropUtils.HIVE_TABLE_CROWD_PORTRAIT_SOURCE_DEVCIE_PROFILE}
       """.stripMargin)
    val sourceCnt = portraitSourceDF.count()
    assert(sourceCnt == 1)

    val portraitEstimationScoreDF = spark.sql(
      s"""
         |select * from ${PropUtils.HIVE_TABLE_CROWD_PORTRAIT_ESTIMATION_SCORE}
       """.stripMargin)
    portraitEstimationScoreDF.show(false)
    val pesCnt = portraitEstimationScoreDF.count()
    assert(pesCnt == 13)
  }

}
