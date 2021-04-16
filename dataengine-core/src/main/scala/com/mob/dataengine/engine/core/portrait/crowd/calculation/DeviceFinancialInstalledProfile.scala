package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils

/**
 * 部分金融标签计算逻辑, 含标签:
 * F0001-F0009
 * rp_sdk_dmp.rp_device_financial_installed_profile|分区2.5亿|6.3G
 * 替换后的老表rp_mobdi_app.rp_device_financial_installed_profile
 * 新表 rp_mobdi_app.timewindow_online_profile|flag=7|timewindow=40
 *
 * @see AbstractFinancialJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("rp_mobdi_app.timewindow_online_profile")
case class DeviceFinancialInstalledProfile(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {

  override protected lazy val _moduleName = "DeviceFinancialInstalledProfile"
  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2
  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(7 -> 40)
  override lazy val flag2Day: Map[String, String] = flag2DayFunc(Map(7 -> 40) )
}
