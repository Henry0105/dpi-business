package com.mob.dataengine.engine.core.profilecal

import java.util.Properties
import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import com.mob.dataengine.commons.{DeviceCacheWriter, DeviceSrcReader}
import com.mob.dataengine.core.bean.IosConfig
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob
import com.mob.dataengine.rpc.RpcClient
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}


object ProfileScore {

  private[this] val logger = Logger.getLogger(ProfileScore.getClass)

  /**
   * 计算基础画像
   *
   */
  def calO2OScore(spark: SparkSession, profileCalTaskJob: ProfileCalTaskJob ): Unit = {

    // 获取需要进行基础画像计算的用户组
    val userIdsStr: String = profileCalTaskJob.getUuidConditionStr

    // 0. 检查缓存表, 加载源表
    // 0.1 检查缓存表"device_profile", 如果未缓存, 则进行重新计算
    /** if (!useMain) {
     * if ("weekly".equals(datetype) || "monthly".equals(datetype)) {
     *   TableCacheManager.cacheDeviceProfile(hiveContext, userIdsStr, firstday, lastday, jobUseridsMap)
     * }
     *}
     */
    // 0.2 加载 IOS/安卓 比率表
    val t_ios_config_df = IosConfig(spark)
    t_ios_config_df.createOrReplaceTempView("mysqltab2")

    // 0.3 检查缓存表"t_warehouse_manager", 如果未缓存, 则重新加载
    val tWarehoueseManagerTableName: String = "t_warehouse_manager_mysql_cached"

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

    val countSql =

      s"""
         |CACHE TABLE $tWarehoueseManagerTableName AS
         |
         |SELECT
         |   a4.userid,
         |   a4.userid AS storeid ,
         |   a4.rank_date,
         |   max(imei_count) as imei_count,
         |   max(mac_count) as mac_count,
         |   max(phone_count) as phone_count,
         |   max(upload_android_count) as upload_android_count,
         |   max(upload_ios_count) as upload_ios_count,
         |   max(upload_count) as upload_count
         |FROM(
         |   SELECT
         |      0 as imei_count,
         |      0 as mac_count,
         |      0 as phone_count,
         |      COUNT(1) as upload_android_count,
         |      0 as upload_ios_count,
         |      COUNT(1) as upload_count,
         |      rank_date,
         |      userid
         |   FROM(
         |      SELECT device,rank_date,userid FROM in_df GROUP BY device,rank_date,userid
         |   ) a0
         |   GROUP BY rank_date,userid
         |UNION ALL
         |   SELECT
         |      COUNT(a1.imei) as imei_count,
         |      0 as mac_count,
         |      0 as phone_count,
         |      0 as upload_android_count,
         |      0 as upload_ios_count,
         |      0 as upload_count,
         |      a1.rank_date,
         |      a1.userid
         |   FROM(
         |      SELECT imei,rank_date,userid FROM device_profile GROUP BY imei,rank_date,userid
         |   ) a1
         |   GROUP BY a1.rank_date,a1.userid
         |UNION ALL
         |   SELECT
         |      0 as imei_count,
         |      COUNT(a2.mac) as mac_count,
         |      0 as phone_count,
         |      0 as upload_android_count,
         |      0 as upload_ios_count,
         |      0 as upload_count,
         |      a2.rank_date,
         |      a2.userid
         |   FROM(
         |      SELECT mac,rank_date,userid FROM device_profile GROUP BY mac,rank_date,userid
         |   ) a2
         |   GROUP BY a2.rank_date,a2.userid
         |UNION ALL
         |   SELECT
         |      0 as imei_count,
         |      0 as mac_count,
         |      COUNT(a3.phone) as phone_count,
         |      0 as upload_android_count,
         |      0 as upload_ios_count,
         |      0 as upload_count,
         |      a3.rank_date,
         |      a3.userid
         |   FROM(
         |      SELECT phone,rank_date,userid FROM device_profile GROUP BY phone,rank_date,userid
         |   ) a3
         |   GROUP BY a3.rank_date,a3.userid
         |) a4
         |GROUP BY a4.rank_date,a4.userid
      """.stripMargin

    spark.sql(countSql)
    //      .createOrReplaceTempView(tWarehoueseManagerTableName)
    //    if (!dataSourceTypeSet.contains("2")) {
    //
    //      var t_warehouse_manager_df = if (O2OConstants.CLEAN_ONE_PLACE_ID_UPDATE_COUNT_TAB !=
    // O2OConstants.CLEAN_LOCATION_FENCE_UPDATE_COUNT_TAB) { // 当一方ID/地理围栏的统计表未统一时, 分别加载并union
    //        spark.read.jdbc(jdbcUrl, O2OConstants.CLEAN_ONE_PLACE_ID_UPDATE_COUNT_TAB, prop)
    //          .select($"sub_task_id", $"mac_count", $"upload_ios_count", $"upload_android_count")
    //          .unionAll(spark.read.jdbc(jdbcUrl, O2OConstants.CLEAN_LOCATION_FENCE_UPDATE_COUNT_TAB, prop)
    //            .select($"sub_task_id", $"mac_count", $"upload_ios_count", $"upload_android_count"))
    //
    //      } else {
    //        // 当一方ID/地理围栏的统计表统一时, 加载任一
    //        spark.read.jdbc(jdbcUrl, O2OConstants.CLEAN_ONE_PLACE_ID_UPDATE_COUNT_TAB, prop)
    //          .select($"sub_task_id", $"mac_count", $"upload_ios_count", $"upload_android_count")
    //      }
    //
    //      t_warehouse_manager_df = t_warehouse_manager_df.select(concat_ws("_", split($"sub_task_id", "_")(1),
    // split($"sub_task_id", "_")(2)).as("userid"), split($"sub_task_id", "_")(2).as("storeid"),
    // 通过切割sub_task_id获得userid和storeid
    //        $"mac_count", $"upload_ios_count", $"upload_android_count", split($"sub_task_id", "_")(0).as("rank_date"))
    //
    //      if (null != jobUseridsMap.getOrElse(OutputCalType.OUTPUT_CAL_TYPE_FURNITURE, null)) {
    //        t_warehouse_manager_df = t_warehouse_manager_df.coalesce(O2OConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
    // .cache()
    //      }
    //      t_warehouse_manager_df.registerTempTable(tWarehoueseManagerTableName)
    //    }


    // 1. 计算
    // 1.1 计算各标签各维度占比(仅含安卓)
    val tmpDF = spark.sql(
      s"""
         |   SELECT
         |      userid,storeid,label,label_id,and_cnt,and_sum,type,
         |      and_cnt/and_sum as perc1
         |   FROM(
         |      SELECT
         |         userid,storeid,label,label_id,and_cnt,type,
         |         sum(and_cnt) over (partition by userid,storeid,label) as and_sum
         |      FROM(
         |         SELECT
         |            userid,storeid,'gender' as label,gender as label_id,count(1) as and_cnt,1 as type
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,gender
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'agebin' as label,agebin as label_id,count(1) as and_cnt,2 as type
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,agebin
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'kids' as label,kids as label_id,count(1) as and_cnt,5 as type
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,kids
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'income' as label,income as label_id,count(1) as and_cnt,3 as type
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,income
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'edu' as label,edu as label_id,count(1) as and_cnt,4 as type
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,edu
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'model_level' as label,model_level as label_id,count(1) as and_cnt,6 as type
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,model_level
         |      ) a1
         |   ) a2
       """.stripMargin
    )
    tmpDF.createOrReplaceTempView("tmptab1")
    // TODO 测试
    /** logger.info(
     *  s"""
     *     |
     *     |------------------------------------------
     *     |"tmptab1" Show:
     *     |
     *     |${tmpDF.show()}
     *     |------------------------------------------
     *     |
     *    """.stripMargin)
     */

    // 1.2 结合 IOS/安卓 比率表进行总量计算
    spark.sql(
      s"""
         |   SELECT
         |      a4.userid,
         |      a4.storeid,
         |      a4.label,
         |      a4.label_id,
         |      a4.and_cnt,
         |      a4.and_sum,
         |      a4.perc1,
         |      a4.type,
         |      a4.ios_avg,
         |      a4.and_avg,
         |      a4.index,
         |      a4.index/a4.index_sum as weight
         |   FROM(
         |      SELECT
         |         a3.userid,
         |         a3.storeid,
         |         a3.label,
         |         a3.label_id,
         |         a3.and_cnt,
         |         a3.and_sum,
         |         a3.perc1,
         |         a3.type,
         |         a3.ios_avg,
         |         a3.and_avg,
         |         a3.index,
         |         sum(a3.index) over (partition by a3.userid,a3.storeid,a3.label) as index_sum
         |      FROM(
         |         SELECT
         |            a2.userid,
         |            a2.storeid,
         |            a2.label,
         |            a2.label_id,
         |            a2.and_cnt,
         |            a2.and_sum,
         |            a2.perc1,
         |            a2.type,
         |            a1.ios_ratio as ios_avg,
         |            a1.android_ratio as and_avg,
         |            a2.perc1/a1.android_ratio*a1.ios_ratio as index
         |         FROM
         |            mysqltab2 a1
         |         JOIN
         |            tmptab1 a2
         |         ON a1.type_name = a2.label
         |         AND a1.key_name = a2.label_id
         |      ) a3
         |   ) a4
       """.stripMargin
    ).createOrReplaceTempView("tmptab2")
    // TODO 测试
    /** logger.info(
     * s"""
     *    |
     *    |------------------------------------------
     *    |"tmptab2" Show:
     *    |
     *    |${spark.table("tmptab2").show()}
     *    |------------------------------------------
     *    |
     *   """.stripMargin)
     */
    spark.sql(
      s"""
         |   SELECT
         |      userid,
         |      storeid,
         |      upload_ios_count*mac_count/upload_android_count as match_ios
         |   FROM
         |      $tWarehoueseManagerTableName
         |   WHERE
         |   userid in ($userIdsStr)
       """.stripMargin
    ).createOrReplaceTempView("tmptab6")
    // TODO 测试
    /** logger.info(
     * s"""
     *    |
     *    |------------------------------------------
     *    |"tmptab6" Show:
     *    |
     *    |${spark.table("tmptab6").show()}
     *    |------------------------------------------
     *    |
     *  """.stripMargin)
     */
    // 1.3 计算最终各标签各维度占比
    val tmptab2JoinTmptab6Sql =
    s"""
       |   SELECT
       |      a4.userid,
       |      a4.storeid,
       |      a4.label,
       |      a4.label_id,
       |      a4.and_cnt,
       |      a4.and_sum,
       |      a4.perc1,
       |      a4.type,
       |      a4.ios_avg,
       |      a4.and_avg,
       |      a4.index,
       |      a4.weight,
       |      a4.match_ios,
       |      a4.ios_cnt,
       |      a4.ios_sum,
       |      (a4.and_cnt + a4.ios_cnt)/(a4.and_sum + a4.ios_sum) as final_pct
       |   FROM(
       |      SELECT
       |         a3.userid,
       |         a3.storeid,
       |         a3.label,
       |         a3.label_id,
       |         a3.and_cnt,
       |         a3.and_sum,
       |         a3.perc1,
       |         a3.type,
       |         a3.ios_avg,
       |         a3.and_avg,
       |         a3.index,
       |         a3.weight,
       |         a3.match_ios,
       |         a3.ios_cnt,
       |         sum(a3.ios_cnt) over (partition by a3.userid,a3.storeid,a3.label) as ios_sum
       |      FROM (
       |         SELECT
       |            a1.userid,
       |            a1.storeid,
       |            a1.label,
       |            a1.label_id,
       |            a1.and_cnt,
       |            a1.and_sum,
       |            a1.perc1,
       |            a1.type,
       |            a1.ios_avg,
       |            a1.and_avg,
       |            a1.index,
       |            a1.weight,
       |            a2.match_ios,
       |            a1.weight*a2.match_ios as ios_cnt
       |         FROM
       |            tmptab2 a1
       |         JOIN
       |            tmptab6 a2
       |         ON a1.storeid = a2.storeid
       |         AND a1.userid = a2.userid
       |      ) a3
       |   ) a4
       """.stripMargin
    val baseScoreTotalCountDF: DataFrame = spark.sql(tmptab2JoinTmptab6Sql)

    // 1.4 有家居画像的计算需求时, 缓存该中间结果表为"tmptab3"
    //    if (null != jobUseridsMap.getOrElse(OutputCalType.OUTPUT_CAL_TYPE_FURNITURE, null)) {
    //     baseScoreTotalCountDF.coalesce(O2OConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
    //       .registerTempTable("tmptab3_tmp_before_cached")
    //      spark.sql("CACHE TABLE tmptab3 AS SELECT * FROM tmptab3_tmp_before_cached")
    //    } else {
    baseScoreTotalCountDF.createOrReplaceTempView("tmptab3")
    //    }

    // 1.5 计算其它标签占比
    spark.sql(
      s"""
         |   SELECT
         |      a1.userid,
         |      a1.storeid,
         |      a1.label,
         |      a1.label_id,
         |      a1.and_cnt
         |   FROM(
         |      SELECT
         |         userid,
         |         storeid,
         |         'cell_factory' as label,
         |         cell_factory as label_id,
         |         count(1) as and_cnt
         |      FROM
         |         device_profile
         |      WHERE userid in ($userIdsStr)
         |      AND match_flag = 1
         |      GROUP BY userid,storeid,cell_factory
         |   ) a1
       """.stripMargin
    ).union(
      spark.sql(
        s"""
           |   SELECT
           |      a1.userid,
           |      a1.storeid,
           |      a1.label,
           |      a1.label_id,
           |      a1.and_cnt
           |   FROM(
           |      SELECT
           |         userid,
           |         storeid,
           |         'cell_factory' as label,
           |         'APPLE' as label_id,
           |         upload_ios_count*mac_count/(upload_android_count+1) as and_cnt
           |      FROM
           |         $tWarehoueseManagerTableName
           |      WHERE
           |      userid in ($userIdsStr)
           |   ) a1
       """.stripMargin
      )
    ).createOrReplaceTempView("tmptab55")

    spark.sql(
      s"""
         |   SELECT
         |      a2.userid,
         |      a2.storeid,
         |      a2.label,
         |      a2.label_id,
         |      a2.and_cnt,
         |      a2.factory_sum,
         |      a2.and_cnt/a2.factory_sum as final_pct
         |   FROM(
         |      SELECT
         |         a1.userid,
         |         a1.storeid,
         |         a1.label,
         |         a1.label_id,
         |         a1.and_cnt,
         |         sum(a1.and_cnt) over (partition by a1.userid,a1.storeid,a1.label) as factory_sum
         |      FROM
         |         tmptab55 a1
         |   ) a2
       """.stripMargin
    ).createOrReplaceTempView("tmptab5")


    // 获取用户下的设备总数
    spark.sql(
      s"""
         |CACHE TABLE tmp_user_device_cnt
         |SELECT
         |    userid,
         |    storeid,
         |    count(1) as user_device_cnt
         |FROM
         |   device_profile
         |WHERE userid in ($userIdsStr)
         |AND match_flag = 1
         |GROUP BY userid,storeid
       """.stripMargin
    )

    // 获取兴趣偏好的统计值
    spark.sql(
      s"""
         |SELECT
         |    a.userid,
         |    a.storeid,
         |    a.label,
         |    a.label_id,
         |    a.and_cnt,
         |    b.user_device_cnt,
         |    a.and_cnt/b.user_device_cnt as final_pct
         |FROM(
         |    SELECT
         |       userid,storeid,'group_list' as label,group_seg as label_id,
         |       count(1) as and_cnt
         |    FROM(
         |        SELECT
         |            userid,
         |            storeid,
         |            group_list
         |        FROM
         |           device_profile
         |        WHERE userid in ($userIdsStr)
         |        AND match_flag = 1
         |    ) t
         |    LATERAL VIEW explode(split(group_list,',')) tmpTable As group_seg
         |    WHERE
         |        group_seg not in ('other','-1','OTHER','其他','未知','unknown','UNKNOWN','')
         |    GROUP BY
         |        userid,
         |        storeid,
         |        group_seg
         |)a
         |INNER JOIN
         |    tmp_user_device_cnt b
         |ON
         |    a.userid = b.userid
         |    AND a.storeid = b.storeid
       """.stripMargin
    ).createTempView("tmptab7")


    spark.sql(
      s"""
         |   SELECT
         |      a2.userid,
         |      a2.storeid,
         |      a2.label,
         |      a2.label_id,
         |      a2.and_cnt,
         |      a2.and_sum,
         |      a2.and_cnt/a2.and_sum as final_pct
         |   FROM(
         |      SELECT
         |         userid,storeid,label,label_id,and_cnt,
         |         sum(and_cnt) over (partition by userid,storeid,label) as and_sum
         |      FROM(
         |         SELECT
         |            userid,storeid,'segment' as label,segment as label_id,
         |            count(1) as and_cnt
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,segment
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'model' as label,model as label_id,
         |            count(1) as and_cnt
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP by userid,storeid,model
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'carrier' as label,carrier as label_id,
         |            count(1) as and_cnt
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,carrier
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'network' as label,network as label_id,
         |            count(1) as and_cnt
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,network
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'screensize' as label,screensize as label_id,
         |            count(1) as and_cnt
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,screensize
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'sysver' as label,sysver as label_id,
         |            count(1) as and_cnt
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,sysver
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'occupation' as label,occupation as label_id,
         |            count(1) as and_cnt
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,occupation
         |      UNION ALL
         |         SELECT
         |            userid,storeid,'car' as label,car as label_id,
         |            count(1) as and_cnt
         |         FROM
         |            device_profile
         |         WHERE userid in ($userIdsStr)
         |         AND match_flag = 1
         |         GROUP BY userid,storeid,car
         |      ) a1
         |   ) a2
       """.stripMargin
    ).createOrReplaceTempView("tmptab4")


    spark.sqlContext.uncacheTable("device_profile")
    spark.sqlContext.dropTempTable("device_profile")
    // 1.6 聚合标签总结果
    spark.sql(
      s"""
         |   SELECT
         |      userid,storeid,label,label_id,
         |      cast(final_pct/sum(final_pct) over(partition by userid,storeid,label) as decimal(21,19)) as final_pct
         |   FROM(
         |      SELECT
         |         userid,storeid,label,label_id,and_cnt,final_pct
         |      FROM
         |         tmptab3
         |   UNION ALL
         |      SELECT
         |         userid,storeid,label,label_id,and_cnt,final_pct
         |      FROM
         |         tmptab5
         |   UNION ALL
         |      SELECT
         |         userid,storeid,label,label_id,and_cnt,final_pct
         |      FROM
         |         tmptab4
         |   ) a
         |   WHERE label_id not in ('other','-1','OTHER','其他','未知','unknown','UNKNOWN','')
         |   AND final_pct is not null
         |   AND final_pct >0
       """.stripMargin).createTempView("final_tmp_before_cached")

    val baseScoreFinalDF: DataFrame = spark.sql(
      s"""
         |SELECT
         |    userid,storeid,label,label_id,final_pct
         |FROM
         |    final_tmp_before_cached
         |UNION ALL
         |SELECT
         |    userid,storeid,label,label_id,final_pct
         |FROM
         |    tmptab7
       """.stripMargin)

    // 有导出HDFS任务时, 缓存该表
    baseScoreFinalDF
      .createOrReplaceTempView("base_score_final_tmp_before_cached")
    spark.sql("CACHE TABLE base_score_final AS SELECT * FROM base_score_final_tmp_before_cached")


    spark.sql(
      s"""
         |
         |SELECT * FROM base_score_final
         |
      """.stripMargin)
      .coalesce(100)
      .createOrReplaceTempView("out_df")

    insertBaseScore2Hive(spark, "out_df", profileCalTaskJob.jobCommon.day,
      profileCalTaskJob.userIdUUIDMap)

    baseScoreFinalDF.unpersist()

    // 2. 结果输出
    //    // 2.1 输出至基础画像中间表 rp_mobeye_o2o.ecology_result_demo_score_o2o
    //    val outputSql: String =
    //    s"""
    //       |   INSERT OVERWRITE TABLE
    //       |      ${O2OConstants.DB_NAME}.ecology_result_demo_score_o2o partition (rank_date='$insertDate',userid)
    //       |   SELECT
    //       |      device,0 score,0 decile,country,province,city,
    //       |      gender,agebin,segment,edu,kids,income,cell_factory,
    //       |      model,model_level,carrier,network,screensize,sysver,
    //       |      match_flag,permanent_country,permanent_province,
    //       |      permanent_city,occupation,house,repayment,car,workplace,
    //       |      residence,applist,married,storeid,industry,userid
    //       |   FROM score_out_df
    //     """.stripMargin
    //
    //    OutputUtils.insertOverwriteToHiveTable(spark,
    //      sourceTableName = "device_profile",
    //      outputFileNums = O2OConstants.EXPORT_DATA_BLOCK_NUM_ACC_TO_DEVICE_NUM,
    //      outputTableTmpName = "score_out_df",
    //      sqlText = outputSql)
    //
    //    // 2.2 输出至基础画像结果表 rp_mobeye_o2o.mobeye_o2o_base_score_[daily/weekly/monthly]
    //    val baseScoreOutputFinalSql: String =
    //      s"""
    //         |   INSERT OVERWRITE TABLE
    //         |      ${O2OConstants.PART_OUTPUT_TAB_NAME_MOBEYE_O2O_BASE_SCORE}_$datetype
    // partition (day=$insertDate,userid)
    //         |   SELECT
    //         |      storeid,label,label_id,final_pct,userid
    //         |   FROM base_score_final_tmp
    //     """.stripMargin
    //
    //
    //    OutputUtils.insertOverwriteToHiveTable(spark,
    //      sourceTableName = "base_score_final",
    //      outputFileNums = O2OConstants.EXPORT_DATA_BLOCK_NUM_TEN,
    //      outputTableTmpName = "base_score_final_tmp",
    //      sqlText = baseScoreOutputFinalSql)
    //
    //    // 3. 结尾工作(释放缓存表, 打印日志)
    //    // 3.1 释放t_warehouse_manager的缓存表
    //    if (spark.isCached(s"$tWarehoueseManagerTableName")) {
    //      spark.uncacheTable(s"$tWarehoueseManagerTableName")
    //    }
    //    if (null == jobUseridsMap.getOrElse(OutputCalType.OUTPUT_CAL_TYPE_BASE_SCORE_EXPORT_HDFS, null)) {
    //      spark.uncacheTable("base_score_final")
    //    }

  }


  /**
   * 输出基础画像结果到hive
   *
   * @param spark     spark
   * @param tableName 需要输出数据的表
   */
  def insertBaseScore2Hive(spark: SparkSession, tableName: String, day: String,
    userIDUUIDMap: Map[String, String]): Unit = {

    import spark._

    val userIDUUIDMapString = s"""map(${userIDUUIDMap.toList.map(item => item._1 + "," + item._2).mkString(",")})"""

    // 动态分区设置
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql(
      s"""
         | INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_MOBEYE_O2O_BASE_SCORE_DAILY}
         | PARTITION(day=$day,userid)
         | SELECT storeid,label AS type,label_id AS sub_type,final_pct AS percent,$userIDUUIDMapString[userid] as userid
         | FROM $tableName
           """.stripMargin)

  }
}
