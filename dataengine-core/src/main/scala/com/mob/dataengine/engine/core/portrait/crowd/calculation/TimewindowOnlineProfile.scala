package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 部分金融标签计算逻辑, 含标签:
 * F0010, F1036-F1039
 * rp_sdk_dmp.timewindow_online_profile|分区4.8亿|5.9G|FLAG=10/TIMEWINDOW=1
 * rp_sdk_dmp.timewindow_online_profile|分区1.8亿|1.9G|FLAG=14/TIMEWINDOW=30
 * rp_sdk_dmp.timewindow_online_profile|分区1.5亿|1.1G|FLAG=15/TIMEWINDOW=30
 *
 * Sum计算逻辑: 各标签数量加总
 * Cnt计算逻辑: 各标签设备去重计数
 *
 * @see AbstractTimewindowProfileJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("rp_mobdi_app.timewindow_online_profile")
case class TimewindowOnlineProfile(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {

  import spark._
  import spark.implicits._
  override protected lazy val _moduleName = "TimewindowOnlineProfile"

  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2

  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(
    7 -> 40,
    0 -> 30,
    3 -> 30
  )

  override lazy val flag2Day: Map[String, String] = flag2DayFunc(
    Map(
      7 -> 40,
      0 -> 30,
      3 -> 30
    )
  )


  @transient
  override protected lazy val finalDF: DataFrame =
    joinedDF.groupBy("uuid", "feature")
      .agg(
        sum("cnt").as("sum"),
        countDistinct("device").as("cnt")
      )
      .filter(
        $"feature".isNotNull &&
          trim($"feature").notEqual("")
      )
      .select($"uuid",
        callUDF("toLabel", $"feature").as("label"),
        $"feature".as("label_id"),
        $"cnt", $"sum")

}
