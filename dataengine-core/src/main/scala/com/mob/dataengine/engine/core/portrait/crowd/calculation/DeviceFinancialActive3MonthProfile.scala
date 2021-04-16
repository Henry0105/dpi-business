package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils

/**
 * 部分金融标签计算逻辑, 含标签:
 * F1015-F1021
 * rp_sdk_dmp.rp_device_financial_active_3month_profile|分区7978万|2.0G
 * 替换后的老表rp_mobdi_app.rp_device_financial_active_3month_profile
 * 新表 rp_mobdi_app.timewindow_online_profile|flag=3|timewindow=30
 *
 * @see AbstractFinancialJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("rp_mobdi_app.timewindow_online_profile")
case class DeviceFinancialActive3MonthProfile(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {

  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2
  override protected lazy val _moduleName = "DeviceFinancialActive3MonthProfile"
  override lazy val flag2Day: Map[String, String] = flag2DayFunc(Map(3 -> 90))
  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(3 -> 90)
}
