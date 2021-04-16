package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 部分旅游标签计算逻辑, 含标签:
 * LF000
 * rp_sdk_dmp.app_active_weekly|分区9亿|22.0G
 * dm_sdk_mapping.apppkg_info|无分区4447.4万|760.1M(过滤cate_l2为"在线旅行"或"旅游出行其他"后仅58条)
 * dm_sdk_mapping.app_active_weekly_penetrance_ratio|分区617.3万|92.4M(暂时未用)
 *
 * Sum计算逻辑: 非去重设备计数
 * Cnt计算逻辑: 去重设备计数
 *
 * @see AbstractJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("rp_sdk_dmp.app_active_weekly,dm_sdk_mapping.apppkg_info")
case class AppActiveWeekly(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractJob(jobContext, sourceData) {

  import spark._
  import spark.implicits._
  import spark.sparkContext.broadcast

  @transient private[this] val logger = Logger.getLogger(this.getClass)

  override protected lazy val tableName: String = PropUtils.HIVE_TABLE_APP_ACTIVE_WEEKLY
  /* 最后分区 */
  private lazy val lastPar: String = sql(s"show partitions $tableName").collect().map(_.getAs[String](0)).last

  override protected lazy val originalDFLoadSQL: String =
    s"select device, apppkg from $tableName where $lastPar"
  /* 筛选出"在线旅行"或"旅游出行其他"类别的apppkg */
  private val apppkgSet: Broadcast[Set[String]] =
    if (needCal) {
      broadcast(
        sql(
          s"""
             |select apppkg from ${PropUtils.HIVE_TABLE_APPPKG_INFO}
             |where cate_l2 in ('在线旅行', '旅游出行其他')
             |and apppkg is not null and apppkg <> ''""".stripMargin)
          .collect().map(_.getAs[String](0)).toSet)
    } else broadcast(Set.empty[String])


  @transient override protected lazy val originalDF: DataFrame = loadOriginalDF
  @transient override protected lazy val joinedDF: DataFrame = loadJoinedDF
  @transient override protected lazy val groupedDF: DataFrame = emptyDataFrame
  @transient override protected lazy val finalDF: DataFrame =
    joinedDF.groupBy("uuid", "apppkg")
      .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
      .select($"uuid", lit("LF000").as("label"), $"apppkg".as("label_id"), $"cnt", $"sum")

  override protected def clear(moduleName: String): Unit = {
    logger.info(s"$moduleName|nothing to clear...")
  }

  /* 加载底层数据, 通过"在线旅行"或"旅游出行其他"类别的apppkg列表进行预过滤 */
  override protected def loadOriginalDF: DataFrame = {
    if (needCal) {
      logger.info(originalDFLoadSQL)
      sql(originalDFLoadSQL)
        .filter(
          r =>
            sourceData.get.deviceBF.value.mightContainString(r.getAs[String]("device")) &&
              apppkgSet.value.contains(r.getAs[String]("apppkg")))
    } else emptyDataFrame
  }

  override protected lazy val _moduleName = "AppActiveWeekly"
}
