package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.{DeviceCacheWriter, DeviceSrcReader}
import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import com.mob.dataengine.engine.core.location.O2OPoiType
import com.mob.dataengine.engine.core.location.enu.OutputOperationTypeEnu
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob
import com.mob.dataengine.rpc.RpcClient
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 */
object ProfileHomeWorkFrequency {

    private[this] val logger = Logger.getLogger(ProfileHomeWorkFrequency.getClass)

  def calO2OHomeWorkFrequency(spark: SparkSession,
                              profileCalTaskJob: ProfileCalTaskJob,
                              insertDate: String = ProfileConstants.DEFAULT_DAY): Unit = {

    // 获取需要进行基础画像计算的用户组
    val userIdStr: String = profileCalTaskJob.getUuidConditionStr

    val lastPartitionHomeWork = spark.sql(
      s"show partitions ${PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK}")
      .collect.toList.last.getString(0)
    val lastPartitionFreq = spark.sql(s"show partitions ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}")
      .collect.toList.last.getString(0)
    // 0. 检查缓存表, 加载源表

    val inDeviceDf = profileCalTaskJob.getInputDF()

    inDeviceDf.createOrReplaceTempView("in_df")
    val deviceProfileDf = DeviceMappingBloomFilter.deviceMapping(spark, inDeviceDf)
    //    deviceProfileDf.createOrReplaceTempView("out_df")
    deviceProfileDf.createOrReplaceTempView("device_profile_tmp")

    spark.sql(
      """
        |
        |CACHE TABLE device_profile AS
        |SELECT * FROM device_profile_tmp
        |
      """.stripMargin)
    // 0.3 "dw_base_poi_l1_geohash_cached", 缓存

    spark.sql(
      s"""
        |
        |CACHE TABLE dw_base_poi_l1_geohash_cached AS
        |SELECT
        |g7,lat,lon,name,type_id
        |FROM ${PropUtils.HIVE_TABLE_DW_BASE_POI_L1_GEOHASH}
        |
      """.stripMargin)

    // 关联常去地
    spark.sql(
      s"""
         |   SELECT
         |      s.*,
         |      named_struct('lat', lat_freq, 'lon', lon_freq, 'province',
         |        province_freq, 'city', city_freq, 'area', area_freq) as frequency,
         |      named_struct('lat', lat_home, 'lon', lon_home, 'province',
         |        province_home, 'city', city_home, 'area', area_home) as home,
         |      named_struct('lat', lat_work, 'lon', lon_work, 'province',
         |        province_work, 'city', city_work, 'area', area_work) as work
         |   FROM(
         |      SELECT * FROM
         |      device_profile
         |      WHERE userid in ($userIdStr)
         |   ) s
         |   JOIN (
         |      SELECT
         |         freq.device
         |         ,COALESCE(hw.lat_home, freq.lat_home) AS lat_home
         |         ,COALESCE(hw.lon_home, freq.lon_home) AS lon_home
         |         ,COALESCE(hw.lat_work, freq.lat_work) AS lat_work
         |         ,COALESCE(hw.lon_work, freq.lon_work) AS lon_work
         |         ,freq.lat_freq, freq.lon_freq
         |         ,freq.province_freq, freq.city_freq, freq.area_freq
         |         ,COALESCE(hw.province_home, freq.province_home) AS province_home
         |         ,COALESCE(hw.city_home, freq.city_home) AS city_home
         |         ,COALESCE(hw.area_home, freq.area_home) AS area_home
         |         ,COALESCE(hw.province_work, freq.province_work) AS province_work
         |         ,COALESCE(hw.city_work, freq.city_work) AS city_work
         |         ,COALESCE(hw.area_work, freq.area_work) AS area_work
         |      FROM (
         |         SELECT device
         |             ,NULL AS lat_home, NULL AS lon_home
         |             ,NULL AS lat_work, NULL AS lon_work
         |             ,cast(lat as string) AS lat_freq, cast(lon as string) AS lon_freq
         |             ,province AS province_freq, city AS city_freq, area AS area_freq
         |             ,NULL AS province_home, NULL AS city_home, NULL AS area_home
         |             ,NULL AS province_work, NULL AS city_work, NULL AS area_work
         |         FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}
         |         WHERE $lastPartitionFreq
         |      ) freq
         |      LEFT JOIN
         |      (
         |         SELECT device
         |             ,cast(lat_home as string) as lat_home, cast(lon_home as string) as lon_home
         |             ,cast(lat_work as string) lat_work, cast(lon_work as string) lon_work
         |             ,NULL AS lat_freq, NULL AS lon_freq
         |             ,NULL AS province_freq, NULL AS city_freq, NULL AS area_freq
         |             ,province_home, city_home, area_home
         |             ,province_work, city_work, area_work
         |         FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK}
         |         WHERE $lastPartitionHomeWork
         |      ) hw
         |      ON freq.device = hw.device
         |      GROUP BY freq.device
         |         ,COALESCE(hw.lat_home, freq.lat_home)
         |         ,COALESCE(hw.lon_home, freq.lon_home)
         |         ,COALESCE(hw.lat_work, freq.lat_work)
         |         ,COALESCE(hw.lon_work, freq.lon_work)
         |         ,freq.lat_freq, freq.lon_freq
         |         ,freq.province_freq, freq.city_freq, freq.area_freq
         |         ,COALESCE(hw.province_home, freq.province_home)
         |         ,COALESCE(hw.city_home, freq.city_home)
         |         ,COALESCE(hw.area_home, freq.area_home)
         |         ,COALESCE(hw.province_work, freq.province_work)
         |         ,COALESCE(hw.city_work, freq.city_work)
         |         ,COALESCE(hw.area_work, freq.area_work)
         |   ) t
         |   ON s.device=t.device
       """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("mobeye_o2o_homework_frequency_tmp_before_cached")

    spark.sqlContext.uncacheTable("device_profile")
    spark.sqlContext.dropTempTable("device_profile")

    spark.sql("CACHE TABLE mobeye_o2o_homework_frequency AS SELECT * " +
      "FROM mobeye_o2o_homework_frequency_tmp_before_cached")

    // 获取常去地类型
    /** val inputPoiSql = "SELECT g7,lat,lon,name,type_id FROM dw_base_poi_l1_geohash_cached"
     * val inputLbsSql =
     *   s"""
     *      |   SELECT frequency.lat,frequency.lon
     *      |   FROM mobeye_o2o_homework_frequency
     *      |   WHERE frequency.lat IS NOT NULL
     *      |   GROUP BY frequency.lat,frequency.lon
     *      |UNION ALL
     *      |   SELECT home.lat,home.lon
     *      |   FROM mobeye_o2o_homework_frequency
     *      |   WHERE home.lat IS NOT NULL
     *      |   GROUP BY home.lat,home.lon
     *      |UNION ALL
     *    |   SELECT work.lat,work.lon
     *    |   FROM mobeye_o2o_homework_frequency
     *    |   WHERE work.lat IS NOT NULL
     *    |   GROUP BY work.lat,work.lon
     * """.stripMargin
     * O2OPoiType.lbsPoiJoin(spark, inputPoiSql, inputLbsSql, useMapJoin = true, 200,
     * OutputOperationTypeEnu.cacheTable, "mobeye_o2o_homework_frequency_type", 200)
     */

    // 获取常去地类型
    val inputPoiSql = "SELECT g7,lat,lon,name,type_id FROM dw_base_poi_l1_geohash_cached"
    val inputLbsSql =
      s"""
         |   SELECT frequency.lat,frequency.lon
         |   FROM mobeye_o2o_homework_frequency
         |   WHERE frequency.lat IS NOT NULL
         |   GROUP BY frequency.lat,frequency.lon
      """.stripMargin

    O2OPoiType.lbsPoiJoin(spark, inputPoiSql, inputLbsSql, useMapJoin = false, 200,
      OutputOperationTypeEnu.cacheTable, "mobeye_o2o_frequency_type", 200)

    val inputHomeLbsSql =
      s"""
         |SELECT home.lat,home.lon
         |FROM mobeye_o2o_homework_frequency
         |WHERE home.lat IS NOT NULL
         |GROUP BY home.lat,home.lon
       """.stripMargin

    O2OPoiType.lbsPoiJoin(spark, inputPoiSql, inputHomeLbsSql, useMapJoin = false, 200,
      OutputOperationTypeEnu.cacheTable, "mobeye_o2o_home_type", 200)

    val inputWorkLbsSql =
      s"""
         |SELECT work.lat,work.lon
         |FROM mobeye_o2o_homework_frequency
         |WHERE work.lat IS NOT NULL
         |GROUP BY work.lat,work.lon
       """.stripMargin
    O2OPoiType.lbsPoiJoin(spark, inputPoiSql, inputWorkLbsSql, useMapJoin = false, 200,
      OutputOperationTypeEnu.cacheTable, "mobeye_o2o_work_type", 200)

    // 关联常去地类型, 工作地和居住地类型,关联用户画像,生成结果表
    val homeworkFrequencyFinalDF: DataFrame = spark.sql(
      s"""
         |SELECT s.userid, s.storeid, s.device
         |   ,gender
         |   ,agebin
         |   ,segment
         |   ,edu
         |   ,kids
         |   ,income
         |   ,occupation
         |   ,house
         |   ,repayment
         |   ,car
         |   ,married
         |   ,home
         |   ,named_struct('lat',h.type_1.lat,'lon',h.type_1.lon,'name',h.type_1.name) as home_type
         |   ,h.geohash_center5 as home_geohash_center5
         |   ,h.geohash_center6 as home_geohash_center6
         |   ,h.geohash_center7 as home_geohash_center7
         |   ,h.geohash_center8 as home_geohash_center8
         |   ,work
         |   ,named_struct('lat',w.type_2.lat,'lon',w.type_2.lon,'name',w.type_2.name) as work_type
         |   ,w.geohash_center5 as work_geohash_center5
         |   ,w.geohash_center6 as work_geohash_center6
         |   ,w.geohash_center7 as work_geohash_center7
         |   ,w.geohash_center8 as work_geohash_center8
         |   ,frequency
         |   ,t.geohash_center5 as freq_geohash_center5
         |   ,t.geohash_center6 as freq_geohash_center6
         |   ,t.geohash_center7 as freq_geohash_center7
         |   ,t.geohash_center8 as freq_geohash_center8
         |   ,named_struct('lat',t.type_1.lat,'lon',t.type_1.lon,'name',t.type_1.name) as type_1
         |   ,named_struct('lat',t.type_2.lat,'lon',t.type_2.lon,'name',t.type_2.name) as type_2
         |   ,named_struct('lat',t.type_3.lat,'lon',t.type_3.lon,'name',t.type_3.name) as type_3
         |   ,named_struct('lat',t.type_4.lat,'lon',t.type_4.lon,'name',t.type_4.name) as type_4
         |   ,named_struct('lat',t.type_5.lat,'lon',t.type_5.lon,'name',t.type_5.name) as type_5
         |   ,named_struct('lat',t.type_6.lat,'lon',t.type_6.lon,'name',t.type_6.name) as type_6
         |   ,named_struct('lat',t.type_7.lat,'lon',t.type_7.lon,'name',t.type_7.name) as type_7
         |   ,named_struct('lat',t.type_8.lat,'lon',t.type_8.lon,'name',t.type_8.name) as type_8
         |   ,named_struct('lat',t.type_9.lat,'lon',t.type_9.lon,'name',t.type_9.name) as type_9
         |   ,named_struct('lat',t.type_10.lat,'lon',t.type_10.lon,'name',t.type_10.name) as type_10
         |FROM
         |   mobeye_o2o_homework_frequency s
         |JOIN
         |   mobeye_o2o_home_type h
         |   ON s.home.lat=h.lat
         |   AND s.home.lon=h.lon
         |JOIN
         |   mobeye_o2o_work_type w
         |   ON s.work.lat=w.lat
         |   AND s.work.lon=w.lon
         |JOIN
         |   mobeye_o2o_frequency_type t
         |   ON s.frequency.lat=t.lat
         |   AND s.frequency.lon=t.lon
       """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
    homeworkFrequencyFinalDF.createOrReplaceTempView("mobeye_o2o_lbs_homeandwork_frequency_tmp_before_cached")
    spark.sql(
      """
        |
        | CACHE TABLE mobeye_o2o_lbs_homeandwork_frequency AS
        | SELECT *
        | FROM mobeye_o2o_lbs_homeandwork_frequency_tmp_before_cached
        |
      """.stripMargin
    )

    val lbsFrequencyFinalDF = spark.sql(
      s"""
         |   SELECT
         |      storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment
         |      ,car,married,frequency,freq_geohash_center5,freq_geohash_center6,freq_geohash_center7
         |      ,freq_geohash_center8
         |      ,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10,userid
         |   FROM
         |      mobeye_o2o_lbs_homeandwork_frequency
         |   WHERE userid in ($userIdStr)
       """.stripMargin
    )
    val lbsHomeWorkFinalDF = spark.sql(
      s"""
         |   SELECT storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment,car,married
         |      ,home,home_type,home_geohash_center5,home_geohash_center6,home_geohash_center7,home_geohash_center8
         |      ,work,work_type,work_geohash_center5,work_geohash_center6,work_geohash_center7,work_geohash_center8
         |      ,userid
         |   FROM
         |      mobeye_o2o_lbs_homeandwork_frequency
         |   WHERE userid in ($userIdStr)
         |   GROUP BY storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment,car,married
         |      ,home,home_type,home_geohash_center5,home_geohash_center6,home_geohash_center7,home_geohash_center8
         |      ,work,work_type,work_geohash_center5,work_geohash_center6,work_geohash_center7,work_geohash_center8
         |      ,userid
         """.stripMargin
    )
    // 有导出HDFS任务时, 缓存该表
    lbsFrequencyFinalDF.coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("lbs_frequency_final_tmp_before_cached")
    spark.sql("CACHE TABLE lbs_frequency_final AS SELECT * FROM lbs_frequency_final_tmp_before_cached")
    lbsHomeWorkFinalDF.coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("lbs_homework_final_tmp_before_cached")
    spark.sql("CACHE TABLE lbs_homework_final AS SELECT * FROM lbs_homework_final_tmp_before_cached")

    spark.sql(
      s"""
         |
         |SELECT * FROM lbs_frequency_final
         |
      """.stripMargin)
      .coalesce(100)
      .createOrReplaceTempView("fre_out_df")

    insertFrequency2Hive(spark, "fre_out_df", profileCalTaskJob.jobCommon.day,
      profileCalTaskJob.userIdUUIDMap)

    spark.sql(
      s"""
         |
         |SELECT * FROM lbs_homework_final
         |
      """.stripMargin)
      .coalesce(100)
      .createOrReplaceTempView("home_work_out_df")

    insertHomeWork2Hive(spark, "home_work_out_df", profileCalTaskJob.jobCommon.day,
      profileCalTaskJob.userIdUUIDMap
    )


    // val frequencyFinalSql: String =
    //   s"""
    //      |   INSERT OVERWRITE TABLE ${O2OConstants.DB_NAME}.mobeye_o2o_lbs_frequency_$datetype
    //      |   partition(rank_date=$insertDate,userid)
    //      |   SELECT
    //      |      storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment
    //      |      ,car,married,frequency,freq_geohash_center5,freq_geohash_center6,freq_geohash_center7,
    //      |      freq_geohash_center8
    //      |      ,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10,userid
    //      |   FROM
    //      |      lbs_frequency_out_df
    //    """.stripMargin
    //
    //    OutputUtils.insertOverwriteToHiveTable(spark,
    //      sourceTableName = "lbs_frequency_final",
    //      outputFileNums = O2OConstants.EXPORT_DATA_BLOCK_NUM_ACC_TO_DEVICE_NUM,
    //      outputTableTmpName = "lbs_frequency_out_df",
    //      sqlText = frequencyFinalSql)
    //
    //    // 合并小文件输出
    //  val homeworkFinalSql: String =
    //    s"""
    //       |   INSERT OVERWRITE TABLE ${O2OConstants.DB_NAME}.mobeye_o2o_lbs_homeandwork_$datetype
    //       | partition(rank_date=$insertDate,userid)
    //       |   SELECT storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment,car,married
    //       |      ,home,home_type,home_geohash_center5,home_geohash_center6,home_geohash_center7,home_geohash_center8
    //       |      ,work,work_type,work_geohash_center5,work_geohash_center6,work_geohash_center7,work_geohash_center8
    //       |      ,userid
    //       |   FROM
    //       |      lbs_homeandwork_out_df
    //       """.stripMargin
    //
    //    OutputUtils.insertOverwriteToHiveTable(spark,
    //      sourceTableName = "lbs_homework_final",
    //      outputFileNums = O2OConstants.EXPORT_DATA_BLOCK_NUM_ACC_TO_DEVICE_NUM,
    //      outputTableTmpName = "lbs_homeandwork_out_df",
    //      sqlText = homeworkFinalSql)

    // 释放缓存

    spark.table("mobeye_o2o_homework_frequency").unpersist()
    spark.table("mobeye_o2o_frequency_type").unpersist()
    spark.table("mobeye_o2o_lbs_homeandwork_frequency").unpersist()
    spark.table("mobeye_o2o_home_type").unpersist()
    spark.table("mobeye_o2o_work_type").unpersist()
    spark.table("dw_base_poi_l1_geohash_cached").unpersist()
    spark.sqlContext.dropTempTable("dw_base_poi_l1_geohash_cached")
    spark.table("lbs_frequency_final").unpersist()
    spark.table("lbs_homework_final").unpersist()

  }

  /**
   * 输出常去地
   *
   * @param spark spark
   * @param tableName 临时表名称
   */
  def insertFrequency2Hive(spark: SparkSession, tableName: String, day: String,
    userIDUUIDMap: Map[String, String]): Unit = {

    import spark._

    val userIDUUIDMapString = s"""map(${userIDUUIDMap.toList.map(item => item._1 + "," + item._2).mkString(",")})"""

    // 动态分区设置
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql(
      s"""
         | INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_FREQUENCY_DAILY}
         | PARTITION(rank_date=$day,userid)
         |   SELECT
         |      storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment
         |      ,car,married,frequency,freq_geohash_center5,freq_geohash_center6,freq_geohash_center7
         |      ,freq_geohash_center8
         |      ,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10,
         |      $userIDUUIDMapString[userid] as userid
         | FROM $tableName
           """.stripMargin)

  }

  /**
   * 输出工作地居住地
   *
   * @param spark spark
   * @param tableName 临时表名称
   */
  def insertHomeWork2Hive(spark: SparkSession, tableName: String, day: String,
    userIDUUIDMap: Map[String, String]): Unit = {

    import spark._

    val userIDUUIDMapString = s"""map(${userIDUUIDMap.toList.map(item => item._1 + "," + item._2).mkString(",")})"""

    // 动态分区设置
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql(
      s"""
         | INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_HOMEANDWORK_DAILY}
         | PARTITION(rank_date=$day,userid)
         |   SELECT storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment,car,married
         |      ,home,home_type,home_geohash_center5,home_geohash_center6,home_geohash_center7,home_geohash_center8
         |      ,work,work_type,work_geohash_center5,work_geohash_center6,work_geohash_center7,work_geohash_center8
         |      ,$userIDUUIDMapString[userid] as userid
         | FROM $tableName
           """.stripMargin)

  }
}
