package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.ProfileScoreOutput
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object ProfileAppInfoV2 {

  private[this] val logger = Logger.getLogger(ProfileAppInfo.getClass)
//  private val appListTable = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL
  private val activeMonthTable = PropUtils.HIVE_TABLE_APP_ACTIVE_MONTHLY_PENETRANCE_RATIO
  private val installFullTable = PropUtils.HIVE_TABLE_APP_INSTALL_PENETRANCE_RATIO
  private val tab: String = "\t"

  def calO2OAppInfo(spark: SparkSession,
    profileCalTaskJob: ProfileCalTaskJob,
    insertDate: String = ProfileConstants.DEFAULT_DAY
  ): Unit = {
    val userIdStr: String = profileCalTaskJob.getUuidConditionStr

    // 0.3 获取源表最后分区
    val activeMonthTableLastPartition = spark.sql(s"show partitions $activeMonthTable")
      .collect.toList.last.getString(0)

    val installTableLastPartition = spark.sql(s"show partitions $installFullTable")
      .collect.toList.last.getString(0)

    val inDeviceDf = profileCalTaskJob.getInputDF()
    if ( inDeviceDf.select("device").distinct().count().toInt == 0 ) {
      logger.info(s"<-------------seed data is null!-----------uuid=$userIdStr------->")
      emptyWriteToCSV(spark, profileCalTaskJob.p.output)
    } else {
      val deviceProfileDf = DeviceMappingBloomFilter.deviceMapping(spark, inDeviceDf)
      deviceProfileDf.cache().createOrReplaceTempView("device_profile")

      computeInstallSampleCnt(spark, userIdStr)
      computeInstallPenetrationTGI(spark, installTableLastPartition)
      computeActiveFullCnt(spark, userIdStr, activeMonthTableLastPartition)
      computeActiveSampleCnt(spark, userIdStr, activeMonthTableLastPartition)
      computeActivePenetrationTGI(spark)
      combineInstallActive(spark)

      addAppCateInfo(spark)

      insertAppInfo2Hive(spark, "out_df", profileCalTaskJob.jobCommon.day, profileCalTaskJob.userIdUUIDMap)

      orderLimitThresholdDF(spark, profileCalTaskJob.p.output)

      writeToCSV(spark, profileCalTaskJob.p.output)

      release(spark)
    }

  }


  /*
    获取在装样本cnt等信息
   */
  def computeInstallSampleCnt(spark: SparkSession, userIdStr: String): Unit = {
    // 1. 计算
    spark.sql(
      s"""
         |SELECT userid, device, apppkg
         |FROM (
         |    SELECT device, userid, applist
         |    FROM device_profile
         |    WHERE userid in ($userIdStr)
         |    AND device IS NOT NULL
         |) src_profile_tbl
         |LATERAL VIEW explode(split(applist,',')) myTable AS apppkg
         """.stripMargin
    ).coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("install_sample_tbl")

    // 1.2 各用户店铺下对应的device数量, 即sample_total_cnt
    val installSampleTotalCntDF = computeSampleCnt(spark, "install_sample_tbl")
    installSampleTotalCntDF.cache()
      .createOrReplaceTempView("install_sample_total_cnt")

    // 1.3 各用户店铺下对应国家省份及app的device数量, 即sample_app_cnt
    computeSampleAppCnt(spark, "install_sample_tbl").cache()
      .createOrReplaceTempView("install_sample_app_device_cnt")
  }

  def computeSampleAppCnt(spark: SparkSession, tableName: String): DataFrame = {
    spark.sql(
      s"""
         |SELECT
         |    apppkg,
         |    userid,
         |    COUNT(1) AS app_device_cnt
         |FROM (
         |    SELECT apppkg, userid, device
         |    FROM $tableName
         |    GROUP BY apppkg, userid, device
         |) aa
         |GROUP BY apppkg, userid
       """.stripMargin)
  }

  def computeSampleCnt(spark: SparkSession, tableName: String): DataFrame = {
    spark.sql(
      s"""
         |SELECT userid, COUNT(1) AS device_cnt
         |FROM (
         |    SELECT userid, device
         |    FROM $tableName
         |    GROUP BY userid, device
         |) t
         |GROUP BY userid
       """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
  }

  /*
    获取活跃样本cnt等信息
   */
  def computeActiveSampleCnt(spark: SparkSession, userIdStr: String, lastPar: String): Unit = {
    // 1. 计算
    spark.sql(
      s"""
         |SELECT
         |    userid,
         |    src_tbl.device,
         |    apppkg
         |FROM (
         |   SELECT device, userid
         |   FROM device_profile
         |   WHERE userid in ($userIdStr)
         |   AND device IS NOT NULL
         |) src_tbl
         |JOIN active_full_tbl
         |ON src_tbl.device=active_full_tbl.device
         """.stripMargin
    ).coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("active_sample_tbl")

    // 1.2 各用户店铺下对应的device数量, 即sample_total_cnt
    val activeSampleTotalCntDF = computeSampleCnt(spark, "active_sample_tbl")
    activeSampleTotalCntDF.cache()
      .createOrReplaceTempView("active_sample_total_cnt")

    // 1.3 各用户店铺下对应国家省份及app的device数量, 即sample_app_cnt
    computeSampleAppCnt(spark, "active_sample_tbl").cache()
      .createOrReplaceTempView("active_sample_app_device_cnt")
  }

  /*
    获取活跃总体cnt等信息
   */
  def computeActiveFullCnt(spark: SparkSession, userIdStr: String, lastPar: String): Unit = {
    // 1. 计算
    spark.sql(
      s"""
         |SELECT device, apppkg
         |FROM $activeMonthTable
         |WHERE $lastPar
         |AND apppkg IS NOT NULL
         |AND apppkg <> ''
         |AND device IS NOT NULL
         |AND device <> ''
         """.stripMargin
    ).coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("active_full_tbl")

    // 1.2 各用户店铺下对应的device数量, 即sample_total_cnt
    import spark.implicits._
    val totalCnt: Broadcast[Long] = spark.sparkContext.broadcast(
      spark.sql(
        s"""
           |SELECT COUNT(1) AS device_cnt
           |FROM (
           |    SELECT device
           |    FROM active_full_tbl
           |    GROUP BY device
           |) t
       """.stripMargin).map(row => row.getAs[Long]("device_cnt")).head

      // spark.table("active_full_tbl").select("device").distinct().count()
    )


    // 1.3 各用户店铺下对应国家省份及app的device数量, 即sample_app_cnt
    val activeFullAppDeviceCnt: DataFrame = spark.sql(
      s"""
         |SELECT
         |    apppkg,
         |    COUNT(1) AS app_device_cnt,
         |    CAST(COUNT(1) AS DOUBLE)/${totalCnt.value} AS active_percent
         |FROM (
         |    SELECT apppkg, device
         |    FROM active_full_tbl
         |    GROUP BY apppkg, device
         |) aft_app
         |GROUP BY apppkg
       """.stripMargin)

    activeFullAppDeviceCnt.cache().createOrReplaceTempView("active_full_app_device_cnt")
  }

  /*
    计算在装量及tgi等信息
   */
  def computeInstallPenetrationTGI(spark: SparkSession, installTableLastPartition: String): Unit = {
    spark.sql(
      s"""
         |SELECT
         |    isadc.userid,
         |    isadc.apppkg,
         |    app_device_cnt,
         |    device_cnt,
         |    CAST(app_device_cnt AS double)/device_cnt AS penetration,
         |    CAST(app_device_cnt AS double)/device_cnt/install_percent*100 AS tgi,
         |    install_percent AS percent
         |FROM install_sample_app_device_cnt isadc
         |LEFT JOIN install_sample_total_cnt istc
         |ON isadc.userid = istc.userid
         |JOIN (
         |    SELECT apppkg, install_percent
         |    FROM $installFullTable
         |    WHERE $installTableLastPartition AND zone='cn' AND cate_id='0' AND install_percent>0
         |    GROUP BY apppkg, install_percent
         |) ins_full
         |ON isadc.apppkg = ins_full.apppkg
       """.stripMargin).cache().createOrReplaceTempView("install_penetration_tgi")
  }


  /*
    计算活跃量及tgi等信息
   */
  def computeActivePenetrationTGI(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |SELECT
         |    asadc.userid,
         |    asadc.apppkg,
         |    app_device_cnt,
         |    device_cnt,
         |    CAST(app_device_cnt AS double)/device_cnt AS penetration,
         |    CAST(app_device_cnt AS double)/device_cnt/active_percent*100 AS tgi,
         |    active_percent AS percent
         |FROM active_sample_app_device_cnt asadc
         |LEFT JOIN active_sample_total_cnt astc
         |ON asadc.userid = astc.userid
         |LEFT JOIN (
         |    SELECT apppkg, active_percent
         |    FROM active_full_app_device_cnt
         |    GROUP BY apppkg, active_percent
         |) act_full
         |ON asadc.apppkg = act_full.apppkg
       """.stripMargin).cache().createOrReplaceTempView("active_penetration_tgi")
  }


  /*
    将活跃和在装的渗透率及tgi等信息组合起来
   */
  def combineInstallActive(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |SELECT
         |    userid,
         |    apppkg,
         |    MAX(install_app_device_cnt) AS install_app_device_cnt,
         |    MAX(install_device_cnt) AS install_device_cnt,
         |    MAX(active_app_device_cnt) AS active_app_device_cnt,
         |    MAX(active_device_cnt) AS active_device_cnt,
         |    MAX(install_penetration) AS install_penetration,
         |    MAX(install_tgi) AS install_tgi,
         |    MAX(active_penetration) AS active_penetration,
         |    MAX(active_tgi) AS active_tgi,
         |    MAX(install_percent) AS install_percent,
         |    MAX(active_percent) AS active_percent
         |FROM (
         |    SELECT
         |        userid,
         |        apppkg,
         |        app_device_cnt AS install_app_device_cnt,
         |        device_cnt AS install_device_cnt,
         |        0 AS active_app_device_cnt,
         |        0 AS active_device_cnt,
         |        penetration AS install_penetration,
         |        tgi AS install_tgi,
         |        0 AS active_penetration,
         |        0 AS active_tgi,
         |        percent AS install_percent,
         |        0 AS active_percent
         |    FROM install_penetration_tgi
         |    UNION ALL
         |    SELECT
         |        userid,
         |        apppkg,
         |        0 AS install_app_device_cnt,
         |        0 AS install_device_cnt,
         |        app_device_cnt AS active_app_device_cnt,
         |        device_cnt AS active_device_cnt,
         |        0 AS install_penetration,
         |        0 AS install_tgi,
         |        penetration AS active_penetration,
         |        tgi AS active_tgi,
         |        0 AS install_percent,
         |        percent AS active_percent
         |    FROM active_penetration_tgi
         |) union_tbl
         |GROUP BY userid, apppkg
       """.stripMargin).cache().createOrReplaceTempView("install_active_union")
  }

  def addAppCateInfo(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |SELECT
         |    userid,
         |    icon,
         |    iau.apppkg,
         |    install_app_device_cnt,
         |    install_device_cnt,
         |    active_app_device_cnt,
         |    active_device_cnt,
         |    install_penetration,
         |    install_tgi,
         |    active_penetration,
         |    active_tgi,
         |    appname,
         |    cate_l1,
         |    cate_l2,
         |    cate_l2_id,
         |    install_percent,
         |    active_percent
         |FROM install_active_union iau
         |LEFT JOIN (
         |    SELECT apppkg, MAX(appname) appname,
         |      MAX(cate_l1) cate_l1, MAX(cate_l2) cate_l2,
         |      MAX(cate_l2_id) cate_l2_id
         |    FROM ${PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR}
         |    WHERE version='1000'
         |    GROUP BY apppkg
         |) app_cate_map
         |ON iau.apppkg = app_cate_map.apppkg
         |LEFT JOIN (
         |    SELECT icon, apppkg
         |    FROM ${PropUtils.HIVE_TABLE_APPPKG_INFO}
         |    WHERE name NOT IN ('other','-1','-2','OTHER','其他','未知','unknown','UNKNOWN','')
         |    AND name IS NOT NULL
         |    AND apppkg IS NOT NULL
         |    AND apppkg <> ''
         |) app_info
         |ON iau.apppkg=app_info.apppkg
         """.stripMargin).cache().createOrReplaceTempView("out_df")
  }


  /*
    * 输出媒介
    *
    * @param spark spark
    * @param tableName 临时表名称
    */
  def insertAppInfo2Hive(spark: SparkSession, tableName: String, day: String,
    userIDUUIDMap: Map[String, String]): Unit = {

    import spark._

    val userIDUUIDMapString = s"""map(${userIDUUIDMap.toList.map(item => item._1 + "," + item._2).mkString(",")})"""

    // 动态分区设置
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    /*
      storeid 商铺id, icon 图标链接, name app名称, cate_name 类别名
      radio 活跃渗透率, index  tgi, apppkg  包名, cate_id  类别编号
      cate_l1  一级类别
     */
    sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_APPINFO_DAILY_V2}
         |PARTITION(day=$day,uuid)
         |SELECT
         |    apppkg,
         |    install_app_device_cnt,
         |    install_device_cnt,
         |    active_app_device_cnt,
         |    active_device_cnt,
         |    install_penetration,
         |    install_tgi,
         |    active_penetration,
         |    active_tgi,
         |    appname,
         |    cate_l1,
         |    cate_l2,
         |    icon,
         |    $userIDUUIDMapString[userid] as uuid
         |FROM $tableName
       """.stripMargin)
  }

  def orderLimitThresholdDF(spark: SparkSession, paramOutput: ProfileScoreOutput): Unit = {
    var filterString = "WHERE 1=1"
    if (paramOutput.threshold.nonEmpty) {
      if (paramOutput.threshold.get.get("install").nonEmpty) {
        filterString += s" AND install_percent > ${paramOutput.threshold.get("install")}"
      }
      if (paramOutput.threshold.get.get("active").nonEmpty) {
        filterString += s" AND active_percent > ${paramOutput.threshold.get("active")}"
      }
    }

    val orderString = if (paramOutput.order.nonEmpty) {
      val orderFieldString = paramOutput.order.get.map{ item =>
        val orderValue = if (item("asc") == 1) "asc"
        else "desc"
        s" ${item("field")} $orderValue "
      }.mkString(" ")
      s" ORDER BY $orderFieldString"
    } else ""
    val limitString = if (paramOutput.limit.nonEmpty) {
      s"LIMIT ${paramOutput.limit.get}"
    } else {
      ""
    }

    spark.sql(
      s"""
         |SELECT apppkg,
         |    install_app_device_cnt,
         |    install_device_cnt,
         |    active_app_device_cnt,
         |    active_device_cnt,
         |    install_penetration,
         |    install_tgi,
         |    active_penetration,
         |    active_tgi,
         |    appname,
         |    cate_l1,
         |    cate_l2,
         |    icon
         |FROM out_df
         |$filterString
         |$orderString
         |$limitString
       """.stripMargin).createOrReplaceTempView("out_df_ordered")
  }

  def writeToCSV(spark: SparkSession, paramOutput: ProfileScoreOutput): Unit = {
    val tsvHeader = List("包名", "在装设备量",
        "在装设备总量", "活跃设备量",
        "活跃设备总量", "在装渗透率",
        "在装tgi", "活跃渗透率",
        "活跃tgi", "中文名",
        "一级类别", "二级类别", "图标")

    val jsonDF = spark.sql(
      s"""
         |SELECT concat_ws('$tab',apppkg,install_app_device_cnt,
         |install_device_cnt,active_app_device_cnt,active_device_cnt,install_penetration,
         |install_tgi,active_penetration,active_tgi,appname,cate_l1,cate_l2,icon)
         |FROM out_df_ordered
      """.stripMargin).toDF(tsvHeader.mkString(tab)).cache()

    val csvOptionsMap = Map(
      "header" -> "true",
      "quote" -> "\u0000",
      "sep" -> "\u0001"
    )
    jsonDF.coalesce(1)
      .write.mode(SaveMode.Overwrite).options(csvOptionsMap)
      .csv(paramOutput.hdfsOutput)
  }

  /** 种子数据为空，导出空文件 */
  def emptyWriteToCSV(spark: SparkSession, paramOutput: ProfileScoreOutput): Unit = {
    val tsvHeader = List("包名", "在装设备量",
      "在装设备总量", "活跃设备量",
      "活跃设备总量", "在装渗透率",
      "在装tgi", "活跃渗透率",
      "活跃tgi", "中文名",
      "一级类别", "二级类别", "图标")

    val resultStr = "the seed data is null"
    val jsonDF = spark.sql(
      s"""
         |SELECT cast('$resultStr' as string)
      """.stripMargin).toDF(tsvHeader.mkString(tab)).cache()

    val csvOptionsMap = Map(
      "header" -> "true",
      "quote" -> "\u0000",
      "sep" -> "\u0001"
    )
    jsonDF.coalesce(1)
      .write.mode(SaveMode.Overwrite).options(csvOptionsMap)
      .csv(paramOutput.hdfsOutput)
  }

  def release(spark: SparkSession): Unit = {
    val tmpTableList = List("device_profile", "install_sample_tbl", "install_sample_total_cnt",
      "install_sample_app_device_cnt", "active_sample_tbl",
      "active_sample_total_cnt", "active_sample_app_device_cnt",
      "active_full_tbl", "active_full_app_device_cnt"
    )
    for (tmpTbl <- tmpTableList) {
      spark.sqlContext.uncacheTable(tableName = tmpTbl)
      spark.sqlContext.dropTempTable(tableName = tmpTbl)
    }
  }

}

