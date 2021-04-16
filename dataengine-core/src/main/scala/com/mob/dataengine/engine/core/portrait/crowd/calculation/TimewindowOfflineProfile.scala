package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 部分旅游标签计算逻辑, 含标签:
 * LJ000, LL000-LR000
 * rp_sdk_dmp.timewindow_offline_profile|分区12亿|8.3G|FLAG=6/TIMEWINDOW=30
 * rp_sdk_dmp.timewindow_offline_profile|分区2.9亿|1.3G|FLAG=9/TIMEWINDOW=30
 *
 * Sum计算逻辑: 各标签数量加总
 * Cnt计算逻辑: 各标签设备去重计数
 *
 * @see AbstractTimewindowProfileJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("rp_sdk_dmp.timewindow_offline_profile")
case class TimewindowOfflineProfile(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {

  import spark._
  import spark.implicits._

  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2

  override lazy val flag2Day: Map[String, String] = flag2DayFunc( Map(6 -> 30, 9 -> 30) )

  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(6 -> 30, 9 -> 30)

  @transient
  override protected lazy val finalDF: DataFrame = {
    joinedDF.filter($"cnt".isNotNull && trim($"cnt").notEqual(""))
      .createOrReplaceTempView("timewindow_offline_profile_explode_tmp")

    sql(
      s"""
         |select uuid, feature, t.label_id, t.label_cnt as sum, 1 as cnt
         |from timewindow_offline_profile_explode_tmp
         |LATERAL VIEW explode_tags(cnt) t AS label_id, label_cnt
         |where t.label_id is not null and t.label_id <> ''
         |group by uuid, feature, t.label_id, t.label_cnt
       """.stripMargin
    )
      .groupBy("uuid", "feature", "label_id")
      .agg(sum("cnt").as("cnt"), sum("sum").as("sum"))
      .filter($"feature".isNotNull && trim($"feature").notEqual(""))
      .select($"uuid", callUDF("toLabel", $"feature").as("label"), $"label_id", $"cnt", $"sum")
  }
  override protected lazy val _moduleName = "TimewindowOfflineProfile"
}
