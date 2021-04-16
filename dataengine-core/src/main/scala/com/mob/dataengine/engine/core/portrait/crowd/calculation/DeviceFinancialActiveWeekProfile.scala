package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils

/**
 * 部分金融标签计算逻辑, 含标签:
 * F1022-F1028
 * 老表rp_sdk_dmp.rp_device_financial_active_week_profile|分区1977万|480M
 * 替换后的老表rp_mobdi_app.rp_device_financial_active_week_profile
 * 新表 rp_mobdi_app.timewindow_online_profile|flag=3|timewindow=7
 *
 * @see AbstractFinancialJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("rp_mobdi_app.timewindow_online_profile")
case class DeviceFinancialActiveWeekProfile(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {

  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2
  override protected lazy val _moduleName = "DeviceFinancialActiveWeekProfile"
  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(3 -> 7)
  override lazy val flag2Day: Map[String, String] = flag2DayFunc(Map(3 -> 7) )
}
