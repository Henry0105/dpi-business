package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.{DeviceCacheWriter, DeviceSrcReader}
import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import com.mob.dataengine.engine.core.location.O2OPoiType
import com.mob.dataengine.engine.core.location.enu.OutputOperationTypeEnu
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob
import com.mob.dataengine.rpc.RpcClient
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 来源去向
 */
object ProfileSourceFlow {

  private[this] val logger = Logger.getLogger(ProfileSourceFlow.getClass)

  def calO2OSourceFlow(spark: SparkSession,
                       profileCalTaskJob: ProfileCalTaskJob,
                       insertDate: String, preDay: String, afterDay: String
                      ): Unit = {

    // 获取需要进行基础画像计算的用户组
    val userIdStr: String = profileCalTaskJob.getUuidConditionStr
    val lastPartitionFreq = spark.sql(s"show partitions ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}")
      .collect.toList.last.getString(0)

    val inDeviceDf = profileCalTaskJob.getInputDF()

    inDeviceDf.createOrReplaceTempView("in_df")
    val deviceProfileDf = DeviceMappingBloomFilter.deviceMapping(spark, inDeviceDf)
    //    deviceProfileDf.createOrReplaceTempView("out_df")
    deviceProfileDf.createOrReplaceTempView("device_profile_tmp")

    spark.sql(
      """
        |
        | CACHE TABLE device_profile AS
        | SELECT * FROM device_profile_tmp
        |
      """.stripMargin)

    val clientTime = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd hh:mm:ss")

    spark.sql(
      s"""
         |
         |   CACHE TABLE mobeye_o2o_device_time AS
         |   SELECT device, storeid, userid,'$clientTime' clienttime
         |   FROM device_profile
         |   WHERE match_flag=1
         |   AND device<>'-1'
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

    // 准备基础数据
    spark.sql(
      s"""
         |   SELECT device,storeid,min(clienttime) as mintime,
         |      max(clienttime) as maxtime,userid
         |   FROM mobeye_o2o_device_time
         |   WHERE userid in ($userIdStr)
         |   GROUP BY  device,storeid,userid
       """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("device_profile_max_min_tmp_before_cached")
    spark.sql("CACHE TABLE device_profile_max_min AS SELECT * FROM device_profile_max_min_tmp_before_cached")


    /**
     * 获取客流来源信息
     * 规则：在店内最后一次采集到的时间之前，最后一条数据所在位置，追溯时间范围由参数确定
     * （因为有些用户在库里当天没有数据，为增大数据量，向前追溯）
     */
    // 缓存"dm_sdk_master.sdk_lbs_daily"
    /** spark.sql(
     * s"""
     *    |   SELECT device,lat,lon, begintime,endtime,province,city,area,
     *    |      concat_ws(' ',day,begintime) as firsttime,
     *    |      concat_ws(' ',day,endtime) as lasttime, day
     *    |   FROM dm_sdk_master.sdk_lbs_daily
     *    |   WHERE day<='$afterday' AND day>='$preday' AND plat=1
     * """.stripMargin)
     * .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
     * .createOrReplaceTempView("sdk_lbs_daily_cached_tmp_before_cached")
     * spark.sql("CACHE TABLE sdk_lbs_daily_cached AS SELECT * FROM sdk_lbs_daily_cached_tmp_before_cached")
     */


    spark.sql(
      s"""
         |   SELECT device,storeid,userid,
         |      named_struct('lat', lat, 'lon', lon, 'province', province,
         |      'city', city, 'area', area) as source
         |   FROM(
         |      SELECT s.device,s.storeid,s.userid,t.lat,t.lon,t.province,t.city,t.area
         |         ,row_number() over(partition by s.device,s.storeid,s.userid order by firsttime desc) as rn
         |      FROM
         |         device_profile_max_min s
         |      JOIN (
         |         SELECT
         |            device,lat,lon,start_time,end_time,province,city,area,
         |            concat_ws(' ',day,start_time) as firsttime
         |         FROM
         |            ${PropUtils.HIVE_TABLE_DEVICE_STAYING_DAILY}
         |         WHERE day<='$insertDate' AND day>='$preDay' AND plat=1
         |      ) t
         |      ON s.device=t.device
         |      WHERE regexp_replace(s.maxtime,'-','') > firsttime
         |   ) s1
         |   WHERE rn = 1
      """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("mobeye_o2o_source_tmp_before_cached")
    spark.sql("CACHE TABLE mobeye_o2o_source AS SELECT * FROM mobeye_o2o_source_tmp_before_cached")

    val inputPoiSql = "SELECT g7,lat,lon,name,type_id FROM dw_base_poi_l1_geohash_cached"
    val inputSourceLbsSql =
      """
        |   SELECT source.lat,source.lon
        |   FROM mobeye_o2o_source
        |   WHERE source.lat IS NOT NULL
        |   GROUP BY  source.lat,source.lon
      """.stripMargin
    // 获取来源类型
    O2OPoiType.lbsPoiJoin(spark, inputPoiSql, inputSourceLbsSql, useMapJoin = true, 100,
      OutputOperationTypeEnu.cacheTable, "mobeye_o2o_source_type", 200)

    // 关联客流来源类型,生成中间结果表
    spark.sql(
      s"""
         |   SELECT s.storeid,s.device,s.userid
         |      ,source
         |      ,geohash_center5
         |      ,geohash_center6
         |      ,geohash_center7
         |      ,geohash_center8
         |      ,named_struct('lat',t.type_1.lat,'lon',t.type_1.lon,'name',t.type_1.name) as s_type_1
         |      ,named_struct('lat',t.type_2.lat,'lon',t.type_2.lon,'name',t.type_2.name) as s_type_2
         |      ,named_struct('lat',s.source.lat,'lon',s.source.lon,'name',s.source.area) as s_type_3
         |   FROM
         |      mobeye_o2o_source  s
         |   JOIN
         |      mobeye_o2o_source_type t
         |   ON s.source.lat=t.lat
         |   AND s.source.lon=t.lon
       """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("mobeye_o2o_lbs_source")
    //  .createOrReplaceTempView("mobeye_o2o_lbs_source_tmp_before_cached")
    // spark.sql("CACHE TABLE mobeye_o2o_lbs_source AS SELECT * FROM mobeye_o2o_lbs_source_tmp_before_cached")


    /**
     * 获取客流去向信息
     * 规则：在店内第一次采集到的时间之后，最近一条数据所在位置，追溯时间范围由参数确定
     * （因为有些用户在我库里当天没有数据，为增大数据量，向后追溯）
     */
    spark.sql(
      s"""
         |   SELECT device,storeid,userid,
         |      named_struct('lat', lat, 'lon', lon, 'province', province, 'city', city, 'area', area) as flow
         |   FROM(
         |      SELECT s.device,s.storeid,s.userid,t.lat,t.lon,t.province,t.city,t.area
         |         ,row_number() over(partition by s.device,s.storeid,s.userid order by lasttime ) as rn
         |      FROM
         |         device_profile_max_min s
         |      JOIN(
         |         SELECT
         |            device,lat,lon,start_time,end_time,province,city,area,
         |            concat_ws(' ',day,end_time) as lasttime
         |         FROM
         |            ${PropUtils.HIVE_TABLE_DEVICE_STAYING_DAILY}
         |         WHERE day>='$insertDate' AND day<='$afterDay' AND plat=1
         |      ) t
         |      ON s.device=t.device
         |      WHERE regexp_replace(s.mintime,'-','') < lasttime
         |    ) s1
         |   WHERE rn = 1
       """.stripMargin).createOrReplaceTempView("mobeye_o2o_flow_tmp")
    spark.sql(
      s"""
         |SELECT device, storeid, userid, flow
         |FROM(
         |   SELECT device, storeid, userid, flow,
         |      row_number() over(partition by device, storeid, userid order by rn) as rn
         |   FROM(
         |      SELECT *, 1 as rn
         |      FROM
         |         mobeye_o2o_flow_tmp
         |   UNION ALL
         |      SELECT s.device, s.storeid, s.userid,
         |         named_struct('lat', lat_freq, 'lon', lon_freq, 'province',
         |         province_freq, 'city', city_freq, 'area', area_freq) as flow
         |         ,2 as rn
         |      FROM
         |         device_profile_max_min s
         |      JOIN (
         |          SELECT device
         |              ,CAST(lat AS STRING) AS lat_freq, CAST(lon AS STRING) AS lon_freq
         |             ,province AS province_freq, city AS city_freq, area AS area_freq
         |          FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}
         |          WHERE $lastPartitionFreq
         |      ) t
         |      ON s.device=t.device
         |   ) s
         |) m
         |WHERE rn = 1
      """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("mobeye_o2o_flow_tmp_before_cached")
    spark.sql("CACHE TABLE mobeye_o2o_flow AS SELECT * FROM mobeye_o2o_flow_tmp_before_cached")

    // 获取去向类型
    val inputFlowLbsSql =
      """
        |   SELECT flow.lat,flow.lon
        |   FROM mobeye_o2o_flow
        |   WHERE flow.lat IS NOT NULL
        |   GROUP BY  flow.lat,flow.lon
      """.stripMargin
    // 获取来源类型
    O2OPoiType.lbsPoiJoin(spark, inputPoiSql, inputFlowLbsSql, useMapJoin = true, 100,
      OutputOperationTypeEnu.cacheTable, "mobeye_o2o_flow_type", 200)
    // 关联客流来源类型,生成中间结果表
    spark.sql(
      """
        |   SELECT s.storeid,s.device,s.userid
        |      ,flow
        |      ,geohash_center5
        |      ,geohash_center6
        |      ,geohash_center7
        |      ,geohash_center8
        |      ,named_struct('lat',t.type_1.lat,'lon',t.type_1.lon,'name',t.type_1.name) as f_type_1
        |      ,named_struct('lat',t.type_2.lat,'lon',t.type_2.lon,'name',t.type_2.name) as f_type_2
        |      ,named_struct('lat',s.flow.lat,'lon',s.flow.lon,'name',s.flow.area) as f_type_3
        |   FROM
        |      mobeye_o2o_flow  s
        |   JOIN
        |      mobeye_o2o_flow_type t
        |   ON s.flow.lat=t.lat
        |   AND s.flow.lon=t.lon
      """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("mobeye_o2o_lbs_flow")
    //  .createOrReplaceTempView("mobeye_o2o_lbs_flow_tmp_before_cached")
    // spark.sql("CACHE TABLE mobeye_o2o_lbs_flow AS SELECT * FROM mobeye_o2o_lbs_flow_tmp_before_cached")


    // 关联客流来源和客流去向
    var sourceFlowFinalDF: DataFrame = spark.sql(
      s"""
         |SELECT s.storeid, s.userid, s.device
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
         |   ,source
         |   ,t.geohash_center5 as s_geohash_center5
         |   ,t.geohash_center6 as s_geohash_center6
         |   ,t.geohash_center7 as s_geohash_center7
         |   ,t.geohash_center8 as s_geohash_center8
         |   ,s_type_1
         |   ,s_type_2
         |   ,s_type_3
         |   ,flow
         |   ,m.geohash_center5 as f_geohash_center5
         |   ,m.geohash_center6 as f_geohash_center6
         |   ,m.geohash_center7 as f_geohash_center7
         |   ,m.geohash_center8 as f_geohash_center8
         |   ,f_type_1
         |   ,f_type_2
         |   ,f_type_3
         |   ,hour
         |FROM(
         |   SELECT
         |      device,storeid,hour(clienttime) as hour
         |      ,userid
         |   FROM
         |      mobeye_o2o_device_time
         |   WHERE userid in ($userIdStr)
         |   GROUP BY  device,storeid,hour(clienttime),userid
         |) s
         |JOIN(
         |      SELECT * FROM
         |      device_profile
         |      WHERE userid in ($userIdStr)
         |)  n
         |ON s.device=n.device
         |AND s.userid = n.userid
         |AND s.storeid = n.storeid
         |JOIN
         |   mobeye_o2o_lbs_source t
         |ON s.device=t.device
         |AND s.storeid=t.storeid
         |AND s.userid = t.userid
         |JOIN
         |   mobeye_o2o_lbs_flow m
         |ON s.device=m.device
         |AND s.storeid=m.storeid
         |AND s.userid = m.userid
       """.stripMargin)

    spark.sqlContext.uncacheTable("device_profile")
    spark.sqlContext.dropTempTable("device_profile")

    // 有导出HDFS任务时, 缓存该表
    sourceFlowFinalDF = sourceFlowFinalDF.coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
    sourceFlowFinalDF.createOrReplaceTempView("mobeye_o2o_lbs_soureandflow_tmp_before_cached")
    spark.sql("CACHE TABLE mobeye_o2o_lbs_soureandflow AS SELECT * FROM mobeye_o2o_lbs_soureandflow_tmp_before_cached")

    spark.sql(
      s"""
         |
         |SELECT * FROM mobeye_o2o_lbs_soureandflow
         |
      """.stripMargin)
      .coalesce(100)
      .createOrReplaceTempView("out_df")

    insertSourceFlow2Hive(spark, "out_df", profileCalTaskJob.jobCommon.day,
      profileCalTaskJob.userIdUUIDMap)
    //  // 输出结果表
    //  val sourceFlowFinalSql: String =
    //    s"""
    //       |INSERT OVERWRITE TABLE ${ProfileConstants.DB_NAME}.mobeye_o2o_lbs_soureandflow_$datetype
    // partition(rank_date=$insertDate,userid)
    //   |SELECT storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment,car,married
    //   |   ,source,s_geohash_center5,s_geohash_center6,s_geohash_center7,s_geohash_center8,s_type_1,s_type_2,s_type_3
    //   |   ,flow,f_geohash_center5,f_geohash_center6,f_geohash_center7,f_geohash_center8,f_type_1,f_type_2,f_type_3
    //   |   ,hour,userid
    //   |FROM
    //   |   lbs_soureandflow_out_df
    //   |WHERE userid in ($userIdStr)
    //       """.stripMargin
    //
    //    OutputUtils.insertOverwriteToHiveTable(spark,
    //      sourceTableName = "mobeye_o2o_lbs_soureandflow",
    //      outputFileNums = ProfileConstants.EXPORT_DATA_BLOCK_NUM_ACC_TO_DEVICE_NUM,
    //      outputTableTmpName = "lbs_soureandflow_out_df",
    //      sqlText = sourceFlowFinalSql)

    // spark.uncacheTable("mobeye_o2o_lbs_source")
    // spark.uncacheTable("mobeye_o2o_lbs_flow")
    spark.sqlContext.uncacheTable("dw_base_poi_l1_geohash_cached")
    // spark.uncacheTable("rp_device_location_3monthly_cached")
    spark.sqlContext.uncacheTable("device_profile_max_min")
    spark.sqlContext.uncacheTable("mobeye_o2o_source")
    spark.sqlContext.uncacheTable("mobeye_o2o_flow")
    spark.sqlContext.uncacheTable("mobeye_o2o_source_type")
    spark.sqlContext.uncacheTable("mobeye_o2o_flow_type")
    spark.sqlContext.uncacheTable("mobeye_o2o_device_time")
    spark.sqlContext.uncacheTable("mobeye_o2o_lbs_soureandflow")

  }

  /**
   * 输出来源去向
   *
   * @param spark     spark
   * @param tableName 临时表名称
   */
  def insertSourceFlow2Hive(spark: SparkSession, tableName: String, day: String,
    userIDUUIDMap: Map[String, String]): Unit = {

    import spark._

    val userIDUUIDMapString = s"""map(${userIDUUIDMap.toList.map(item => item._1 + "," + item._2).mkString(",")})"""

    // 动态分区设置
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql(
      s"""
         | INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_MOBEYE_O2O_LBS_SOUREANDFLOW_DAILY}
         | PARTITION(rank_date=$day,userid)
         | SELECT storeid,device,gender,agebin,segment,edu,kids,income,occupation,house,repayment,car,married
         |   ,source,s_geohash_center5,s_geohash_center6,s_geohash_center7,s_geohash_center8,s_type_1,s_type_2,s_type_3
         |   ,flow,f_geohash_center5,f_geohash_center6,f_geohash_center7,f_geohash_center8,f_type_1,f_type_2,f_type_3
         |   ,hour,$userIDUUIDMapString[userid] as userid
         | FROM $tableName
           """.stripMargin)

  }
}
