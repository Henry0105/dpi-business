package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils

/**
 * 部分金融标签计算逻辑, 含标签:
 * F1008-F1014
 * rp_sdk_dmp.rp_device_financial_active_month_profile|分区4292万|1.1G
 * 替换后的老表rp_mobdi_app.rp_device_financial_active_month_profile
 * 新表 rp_mobdi_app.timewindow_online_profile|flag=3|timewindow=30
 *
 * @see AbstractFinancialJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("rp_mobdi_app.timewindow_online_profile")
case class DeviceFinancialActiveMonthProfile(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {

  import spark._
  import spark.implicits._
  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2
  override protected lazy val _moduleName = "DeviceFinancialActiveMonthProfile"
  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(3 -> 30)
  override lazy val flag2Day: Map[String, String] = flag2DayFunc(Map(3 -> 30))
}
