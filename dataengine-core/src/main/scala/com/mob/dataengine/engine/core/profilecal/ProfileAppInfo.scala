package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.{DeviceCacheWriter, DeviceSrcReader}
import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob
import com.mob.dataengine.rpc.RpcClient
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


object ProfileAppInfo {

  private[this] val logger = Logger.getLogger(ProfileAppInfo.getClass)

  def calO2OAppInfo(spark: SparkSession,
    profileCalTaskJob: ProfileCalTaskJob,
    insertDate: String = ProfileConstants.DEFAULT_DAY
  ): Unit = {

    import spark.implicits._

    val userIdStr: String = profileCalTaskJob.getUuidConditionStr

    // 0. 检查缓存表, 加载源表
    // 0.1 检查缓存表"device_profile", 如果未缓存, 则进行重新计算
    /* if (!useMain) {
      * if (("weekly".equals(datetype) || "monthly".equals(datetype)) &&
      *   null == jobUseridsMap.getOrElse(OutputCalType.OUTPUT_CAL_TYPE_BASE_SCORE, null)) {
      *   O2ODeviceProfile.calDeviceProfile(spark, userids, firstday, lastday, jobUseridsMap)
      * }
      * }
      */

    // 0.2 根据dateType确定源表及对应分区
    val activePenetranceTableName = PropUtils.HIVE_TABLE_APP_ACTIVE_WEEKLY_PENETRANCE_RATIO
    val appListTableName = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL
    val installFullTable = PropUtils.HIVE_TABLE_APP_INSTALL_PENETRANCE_RATIO

    // activePenetranceTableName = "rp_sdk_mobeye.app_active_weekly_penetrance_ratio"
    // 0.3 获取源表最后分区
    val activePenetranceTableLastPartition = spark.sql(s"show partitions $activePenetranceTableName")
      .collect.toList.last.getString(0)
    val installTableLastPartition = spark.sql(s"show partitions $installFullTable")
      .collect.toList.last.getString(0)


    val inDeviceDf = profileCalTaskJob.getInputDF()
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

    // 1. 计算
    spark.sql(
      s"""
         |SELECT userid, storeid, device, apppkg, country, province
         |FROM(
         |    SELECT
         |        userid,
         |        storeid,
         |        t1.device,
         |        applist,
         |        country,
         |        province
         |    FROM(
         |       SELECT device, country, province, storeid, userid
         |       FROM device_profile
         |       WHERE userid in ($userIdStr)
         |       AND device IS NOT NULL
         |    ) t1
         |    JOIN(
         |       SELECT device, applist
         |       FROM $appListTableName
         |       WHERE applist IS NOT NULL
         |       AND applist <> ''
         |       AND device IS NOT NULL
         |       AND device <> ''
         |    ) t2
         |    ON t1.device=t2.device
         |) t
         |LATERAL VIEW explode(split(applist,',')) myTable AS apppkg
         """.stripMargin
    ).coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("app_tmptable1_tmp_before_cached")
    spark.sql("CACHE TABLE app_tmptable1 AS SELECT * FROM app_tmptable1_tmp_before_cached")

    spark.sqlContext.uncacheTable("device_profile")
    spark.sqlContext.dropTempTable("device_profile")

    // 1.2 各用户店铺下对应国家省份的device数量, 即cnt2
    val useridStoreidAppinfoCnt2DF = spark.sql(
      s"""
         |   SELECT
         |      userid, storeid, 0 as decile, 'all' as apppkg,
         |      0 as cnt1, count(1) as cnt2, 0 as cnt3, 0 as cnt4,
         |      '' as icon, '' as name, '' as cate_id ,
         |      '' as cate_name, country, province
         |   FROM(
         |      SELECT
         |         country,province,storeid,userid,device
         |         FROM app_tmptable1
         |      GROUP BY country,province,storeid,userid,device
         |   ) t
         |   GROUP BY country,province,storeid,userid
       """.stripMargin)
      .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS) // TODO 取消中间表后可取消缓存
    useridStoreidAppinfoCnt2DF.createOrReplaceTempView("userid_storeid_appinfo_cnt2_cached_tmp_before_cached")
    spark.sql("CACHE TABLE userid_storeid_appinfo_cnt2_cached AS " +
      "SELECT * FROM userid_storeid_appinfo_cnt2_cached_tmp_before_cached")

    /*  1.3 各用户店铺下对应国家省份及app的device数量
      * val useridStoreidAppinfoCnt1DF = spark.table("app_tmptable1")
      *       .groupBy($"userid", $"storeid", $"apppkg", $"country", $"province")
      *       .agg(count($"device").as("cnt1"))
      *       .select($"userid", $"storeid", $"apppkg", $"country", $"province", $"cnt1")
      *       .groupBy($"userid", $"storeid", $"apppkg")
      *       .agg(sum($"cnt1").as("cnt1"))
      *       .select($"userid", $"storeid", $"apppkg", $"cnt1")
      *       .mapPartitions(rowIterator => {
      *         val apppkgInfoMap = apppkgInfoMapBC.value
      *         //val useridStoreidCnt2Sum = useridStoreidCnt2SumBC.value // TODO 取消中间表后可更改
      *         rowIterator.map(row => {
      *           val userid = row.getString(0)
      *           val storeid = row.getString(1)
      *           val apppkg = row.getString(2)
      *           val cnt1 = row.getLong(3)
      *           //var cnt2 = 0 // TODO 取消中间表后可更改
      *           //val combinedKey = userid + "\0001" + storeid // TODO 取消中间表后可更改
      *           // TODO 取消中间表后可更改
      *           /*if (useridStoreidCnt2Sum.contains(combinedKey)) {
      *             cnt2 = useridStoreidCnt2Sum(combinedKey)
      *           }*/
      * if (apppkgInfoMap.contains(apppkg)) {
      * val combinedRow = apppkgInfoMap(apppkg)
      * val icon = combinedRow.getString(1)
      * val name = combinedRow.getString(2)
      * val cateid = combinedRow.getString(3)
      * val cateName = combinedRow.getString(4)

      * apppkg -> (userid, storeid, apppkg, 0, cnt1, 0, 0, 0, icon, name, cateid, cateName)
      * //apppkg -> (userid, storeid, apppkg, 0, cnt1, cnt2, 0, 0, icon, name, cateid, cateName)
      * // TODO 取消中间表后可更改

      * } else apppkg -> (userid, storeid, apppkg, 0, cnt1, 0, 0, 0, "", "", "", "")
      * //} else apppkg -> (userid, storeid, apppkg, 0, cnt1, cnt2, 0, 0, "", "", "", "") // TODO 取消中间表后可更改

      * })
      * }).toDF("userid", "storeid", "apppkg", "decile", "cnt1", "cnt2",
      * "cnt3", "cnt4", "icon", "name", "cate_id", "cate_name")
      *     .coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      */

    val tmpFullDF = spark.sql(
      s"""
         |SELECT apppkg, install_percent
         |FROM $installFullTable
         |WHERE $installTableLastPartition AND zone='cn' AND cate_id='0' AND install_percent > 0
         |GROUP BY apppkg, install_percent
       """.stripMargin)

    tmpFullDF.createOrReplaceTempView("full_tbl_penetration")

    // 1.3 各用户店铺下对应国家省份及app的device数量, 即cnt1
    val useridStoreidAppinfoCnt1DF: DataFrame = spark.sql(
      s"""
         |   SELECT
         |      0 as decile, aa.apppkg,
         |      aa.cnt1, 0 as cnt2, 0 as cnt3, 0 as cnt4,
         |      mm.icon, trim(coalesce(mm.name,'')) as name, coalesce(mm.cate_l2_id,'other') cate_id,
         |      coalesce(mm.cate_l2,'other') cate_name, aa.storeid, aa.userid, aa.country, aa.province,
         |      coalesce(mm.cate_name,'other') cate_l1,
         |      full_tbl_penetration.install_percent install_percent
         |   FROM(
         |      SELECT
         |         apppkg, storeid,
         |         userid, count(1) as cnt1,
         |         country, province
         |      FROM app_tmptable1
         |      GROUP BY apppkg,storeid,userid,country,province
         |   ) aa
         |   LEFT OUTER JOIN(
         |      SELECT * FROM
         |      ${PropUtils.HIVE_TABLE_APPPKG_INFO}
         |      WHERE name NOT IN ('other','-1','-2','OTHER','其他','未知','unknown','UNKNOWN','')
         |      AND name IS NOT NULL
         |      AND apppkg IS NOT NULL
         |      AND apppkg <> ''
         |   ) mm
         |   ON aa.apppkg=mm.apppkg
         |   INNER JOIN full_tbl_penetration
         |   ON aa.apppkg=full_tbl_penetration.apppkg
       """.stripMargin)

    useridStoreidAppinfoCnt1DF.createOrReplaceTempView("userid_storeid_appinfo_cnt1_cached_tmp_before_cached")
    spark.sql("CACHE TABLE userid_storeid_appinfo_cnt1_cached AS SELECT * " +
      "FROM userid_storeid_appinfo_cnt1_cached_tmp_before_cached")

    spark.sqlContext.uncacheTable("app_tmptable1")

    // 1.4 各国家省份的app在装device数量和各国家省份的device总数, 即cnt3和cnt4
    // 取出apppkg的结果并进行广播, 从cnt1中
    val apppkgSetBC: Broadcast[Set[String]] = spark.sparkContext.broadcast(
      spark.table("userid_storeid_appinfo_cnt1_cached").select($"apppkg")
        .map(_.getString(0)).distinct().collect().toSet)

    // 获取需要计算的userids
    val userIdSet: collection.Set[String] = profileCalTaskJob.getUuidSeq.toSet


    val activePenetranceDF = spark.sql(
      s"""
         |   SELECT apppkg, decile, cnt1, cnt2, cnt3, cnt4, country, province
         |   FROM
         |      $activePenetranceTableName
         |   WHERE $activePenetranceTableLastPartition
         |   AND country = 'cn'
         |   AND apppkg IS NOT NULL
         |   AND apppkg <> ''
       """.stripMargin
    ).coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS).distinct()

    val summaryAppinfoCnt3Cnt4RDD: RDD[Row] = activePenetranceDF.rdd.mapPartitions(rowIterator => {

      val apppkgSet = apppkgSetBC.value

      rowIterator.flatMap(row => {

        val apppkg = row.getString(0)
        val decile = if (null == row.get(1)) 0 else row.getInt(1)
        val cnt1 = if (null == row.get(2)) 0L else row.getLong(2)
        val cnt2 = if (null == row.get(3)) 0L else row.getLong(3)
        val cnt3 = if (null == row.get(4)) 0L else row.getLong(4)
        val cnt4 = if (null == row.get(5)) 0L else row.getLong(5)
        val country = row.getString(6)
        val province = row.getString(7)

        userIdSet.map(userid => {

          if (apppkgSet.contains(apppkg) || "all" == apppkg) {

            Row(userid, "", apppkg, decile, cnt1, cnt2, cnt3, cnt4, "", "", "", "", country, province)

          } else null
        }).filter(null != _)
      })
    })

    val summaryAppinfoCnt3Cnt4DF: DataFrame = spark.createDataFrame(summaryAppinfoCnt3Cnt4RDD, StructType(Array(
      StructField("userid", DataTypes.StringType),
      StructField("storeid", DataTypes.StringType),
      StructField("apppkg", DataTypes.StringType),
      StructField("decile", DataTypes.IntegerType),
      StructField("cnt1", DataTypes.LongType),
      StructField("cnt2", DataTypes.LongType),
      StructField("cnt3", DataTypes.LongType),
      StructField("cnt4", DataTypes.LongType),
      StructField("icon", DataTypes.StringType),
      StructField("name", DataTypes.StringType),
      StructField("cate_id", DataTypes.StringType),
      StructField("cate_name", DataTypes.StringType),
      StructField("country", DataTypes.StringType),
      StructField("province", DataTypes.StringType)
    ))).coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
    summaryAppinfoCnt3Cnt4DF.createOrReplaceTempView("summary_appinfo_cnt3_cnt4_cached_tmp_before_cached")
    spark.sql("CACHE TABLE summary_appinfo_cnt3_cnt4_cached AS SELECT * " +
      "FROM summary_appinfo_cnt3_cnt4_cached_tmp_before_cached")

    apppkgSetBC.unpersist()

    // 1.5 输出至中间结果表
//    spark.sql(
//      s"""
//         |   SELECT userid, storeid, country, province, apppkg, decile, cnt1, cnt2, cnt3, cnt4, icon,
//         |     name, cate_id, cate_name
//         |   FROM summary_appinfo_cnt3_cnt4_cached
//         |UNION ALL
//         |   SELECT userid, storeid, country, province, apppkg, decile, cnt1, cnt2, cnt3, cnt4, icon,
//         |     name, cate_id, cate_name
//         |   FROM userid_storeid_appinfo_cnt1_cached
//         |UNION ALL
//         |   SELECT userid, storeid, country, province, apppkg, decile, cnt1, cnt2, cnt3, cnt4, icon,
//         |     name, cate_id, cate_name
//         |   FROM userid_storeid_appinfo_cnt2_cached
//       """.stripMargin
//    ).createOrReplaceTempView("app_info_output_tmp")
    //    val appInfoOutputTmpSql: String =
    //      s"""
    //         |INSERT OVERWRITE TABLE rp_mobeye_o2o.ecology_region_app_info_o2o partition(rank_date=$insertDate,userid)
    //         | SELECT
    //         |    country, province, decile, apppkg,
    //         |    cnt1, cnt2, cnt3, cnt4,
    //         |    icon, name, cate_id, cate_name,
    //         |    storeid, userid
    //         | FROM app_info_output_tmp_tmp
    //       """.stripMargin
    //
    //    OutputUtils.insertOverwriteToHiveTable(spark,
    //      sourceTableName = "app_info_output_tmp",
    //      outputFileNums = ProfileConstants.EXPORT_DATA_BLOCK_NUM_ACC_TO_DEVICE_NUM,
    //      outputTableTmpName = "app_info_output_tmp_tmp",
    //      sqlText = appInfoOutputTmpSql)

    // 2.1 计算cnt2总数并广播
    val useridStoreidCnt2SumBC: Broadcast[collection.Map[String, Long]] = spark.sparkContext.broadcast(

      spark.table("userid_storeid_appinfo_cnt2_cached")

        .groupBy($"userid", $"storeid")

        .agg(sum($"cnt2").as("sum2"))

        .select($"userid", $"storeid", $"sum2")
        .map(row => (row.getString(0) + ProfileConstants.EXPORT_DATA_SEPARATOR + row.getString(1))
          -> (if (null == row.get(2)) 0L else row.getLong(2))).collect.toMap)

    // 2.2 计算cnt3和cnt4总数并广播
//    val apppkgCnt3Cnt4MapBC: Broadcast[collection.Map[String, Row]] = spark.sparkContext.broadcast(
//      spark.table("summary_appinfo_cnt3_cnt4_cached")
//        .filter($"cnt1" === 0 && $"cnt2" === 0)
//        .groupBy($"apppkg")
//        .agg(sum($"cnt3").as("sum3"), sum($"cnt4").as("sum4"))
//        .select($"apppkg", $"sum3", sum($"sum4").over(Window.partitionBy(lit(1))).as("sum4"))
//        .rdd
//        .map(row => {
//          row.getString(0) -> row // apppkg -> row
//        }).collect.toMap)

    import spark.implicits._

    // 2.3 由cnt1和以上三个总数进行聚合
    spark.table("userid_storeid_appinfo_cnt1_cached")

      .groupBy($"userid", $"storeid", $"apppkg")

      .agg(sum($"cnt1").as("sum1"), max($"decile").as("decile"), max($"icon").as("icon"), max($"name").as("name"),
        max($"cate_id").as("cate_id"), max($"cate_name").as("cate_name"), max($"cate_l1").as("cate_l1"),
        max($"install_percent").as("install_percent"))

      .select($"userid", $"storeid", $"apppkg", $"decile", $"icon", $"name",
        $"cate_id", $"cate_name", $"sum1", $"cate_l1", $"install_percent")

      .mapPartitions(rowIterator => {

        val useridStoreidCnt2Sum: collection.Map[String, Long] = useridStoreidCnt2SumBC.value
//        val apppkgCnt3Cnt4Map: collection.Map[String, Row] = apppkgCnt3Cnt4MapBC.value

        rowIterator.map(row => {

          val userid = row.getString(0)
          val storeid = row.getString(1)
          val apppkg = row.getString(2)
          val decile = if (null == row.get(3)) 0 else row.getInt(3)
          val icon = row.getString(4)
          val name = row.getString(5)
          val cateid = row.getString(6)
          val cateName = row.getString(7)
          val cateL1 = row.getString(9)
          val cnt1Sum = if (null == row.get(8)) 0L else row.getLong(8)
          var cnt2Sum = 0L
          val installPercent = if (null == row.get(10)) 0.toDouble else row.getDouble(10)

          val combinedKey = userid + ProfileConstants.EXPORT_DATA_SEPARATOR + storeid

          if (useridStoreidCnt2Sum.contains(combinedKey)) {
            cnt2Sum = useridStoreidCnt2Sum(combinedKey)
          }
          (userid, storeid, apppkg, decile, icon, name, cateid, cateName, cnt1Sum, cnt2Sum, installPercent, cateL1)

//          if (apppkgCnt3Cnt4Map.contains(apppkg)) {
//            val combinedRow = apppkgCnt3Cnt4Map(apppkg)
//            val cnt3Sum = if (null == combinedRow.get(1)) 0L else combinedRow.getLong(1)
//            val cnt4Sum = if (null == combinedRow.get(2)) 0L else combinedRow.getLong(2)
//            (userid, storeid, apppkg, decile, icon, name, cateid, cateName,
          //            cnt1Sum, cnt2Sum, cnt3Sum, cnt4Sum, cateL1)
//          } else null


        }).filter(null != _)

      }).toDF("userid", "storeid", "apppkg", "decile", "icon", "name", "cate_id", "cate_name",
      "cnt1", "cnt2", "install_percent", "cate_l1")
      .createOrReplaceTempView("summary_userid_storeid_appinfo_final_sum")

    // 2.4 计算最终结果
    // AND d.cnt1/d.cnt2>0.001
    val appInfoFinalDF: DataFrame = spark.sql(
      s"""
         |   SELECT
         |      d.userid,d.storeid,d.icon,d.name,d.cate_name,d.cate_l1,
         |      cast(d.cnt1/d.cnt2 as decimal(21,19)) as radio,
         |      (d.cnt1/d.cnt2)/d.install_percent*100 as index,
         |      apppkg,
         |      cate_id
         |   FROM
         |      summary_userid_storeid_appinfo_final_sum d
         |   WHERE d.cnt2>0
         |   AND d.userid in ($userIdStr)
         |
       """.stripMargin)

    // 有导出HDFS任务时, 缓存该表
    appInfoFinalDF.coalesce(ProfileConstants.SPARK_SQL_SHUFFLE_PARTITIONS)
      .createOrReplaceTempView("region_appinfo_final_tmp_before_cached")
    spark.sql("CACHE TABLE region_appinfo_final AS SELECT * FROM region_appinfo_final_tmp_before_cached")

    spark.sql(
      s"""
         |
         |SELECT * FROM region_appinfo_final
         |
      """.stripMargin)
      .coalesce(100)
      .createOrReplaceTempView("out_df")

    insertAppInfo2Hive(spark, "out_df", profileCalTaskJob.jobCommon.day, profileCalTaskJob.userIdUUIDMap)
    // 2.5 输出至最终结果表
    //    val baseScoreOutputFinalSql: String =
    //      s"""
    //         |   INSERT OVERWRITE TABLE
    //         |      ${ProfileConstants.PART_OUTPUT_TAB_NAME_MOBEYE_O2O_REGION_APPINFO}_$datetype
    // partition (day=$insertDate,userid)
    //         |   SELECT
    //         |      storeid,icon,name,cate_name,radio,index,apppkg,cate_id,userid
    //         |   FROM region_appinfo_final_tmp
    //     """.stripMargin
    //
    //    OutputUtils.insertOverwriteToHiveTable(spark,
    //      sourceTableName = "region_appinfo_final",
    //      outputFileNums = ProfileConstants.EXPORT_DATA_BLOCK_NUM_ACC_TO_DEVICE_NUM,
    //      outputTableTmpName = "region_appinfo_final_tmp",
    //      sqlText = baseScoreOutputFinalSql)

    spark.sqlContext.uncacheTable("userid_storeid_appinfo_cnt1_cached")
    spark.sqlContext.uncacheTable("userid_storeid_appinfo_cnt2_cached")
    spark.sqlContext.uncacheTable("summary_appinfo_cnt3_cnt4_cached")
    useridStoreidCnt2SumBC.unpersist()

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
    sql(
      s"""
         | INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_APPINFO_DAILY}
         | PARTITION(day=$day,userid)
         | SELECT storeid,icon,name,cate_name,radio,index,apppkg,cate_id,cate_l1,$userIDUUIDMapString[userid] as userid
         | FROM $tableName
       """.stripMargin)
  }
}
