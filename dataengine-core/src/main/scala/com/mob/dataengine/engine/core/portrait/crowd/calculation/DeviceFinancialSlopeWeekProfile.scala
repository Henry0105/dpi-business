package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils

/**
 * 部分金融标签计算逻辑, 含标签:
 * F1029-F1035
 * rp_sdk_dmp.rp_device_financial_slope_week_profile|分区2122万|513.5M
 * 替换后的老表rp_mobdi_app.rp_device_financial_slope_week_profile
 * 新表 rp_mobdi_app.timewindow_online_profile|flag=3|timewindow=30
 *
 * @see AbstractFinancialJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("rp_mobdi_app.timewindow_online_profile")
case class DeviceFinancialSlopeWeekProfile(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {

  import spark._
  import spark.implicits._
  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2
  override protected lazy val _moduleName = "DeviceFinancialSlopeWeekProfile"
  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(6 -> 7)
  override lazy val flag2Day: Map[String, String] = flag2DayFunc(Map(6 -> 7))
}
