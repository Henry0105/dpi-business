package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 游戏标签计算逻辑, 含标签:
 * F0010, F1036-F1039
 * rp_sdk_dmp.timewindow_online_profile|FLAG=17/游戏类新安装 对应feature中间值为0
 * rp_sdk_dmp.timewindow_online_profile|FLAG=18/游戏类新卸载 对应feature中间为1
 * rp_sdk_dmp.timewindow_online_profile|FLAG=19/游戏类活跃天数 对应feature中间为3
 * rp_sdk_dmp.timewindow_online_profile|FLAG=20/游戏类活跃个数 对应feature中间为2
 * rp_sdk_dmp.timewindow_online_profile|FLAG=21/游戏类app安装个数 对应feature中间为4
 *
 * Sum计算逻辑: 各标签数量加总
 * Cnt计算逻辑: 各标签设备去重计数
 *
 * @see AbstractTimewindowProfileJob
 */
@author("xdzhang")
@createTime("2018-09-05")
@sourceTable("rp_sdk_dmp.timewindow_online_profile")
case class DeviceGamesProfile(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {

  import spark._
  import spark.implicits._
  override protected lazy val _moduleName = "TimewindowOnlineGameProfile"
  override lazy val flag2Day: Map[String, String] = Map()
  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2

  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(0 -> 7)

  val lastDay: String = {
    if (needCal) {
      val lastPars = sql(s"show partitions $tableName").collect().map(_.getAs[String](0)).toSeq
      genFlagDay(0, 7, lastPars)._2
    } else null
  }
  lazy val tableCondition = new TableCondition
  @transient
  override protected lazy val originalDFLoadSQL: String = {
    if (lastDay != null && !lastDay.isEmpty) {
      s"""select device,feature,cnt from ${tableName}
         |where ${tableCondition.toString}
         |and day=${lastDay}
       """.stripMargin
    } else ""
  }

  @transient
  override protected lazy val finalDF: DataFrame =
    joinedDF.groupBy("uuid", "feature")
      .agg(sum("cnt").as("sum"), countDistinct("device").as("cnt"))
      .filter($"feature".isNotNull && trim($"feature").notEqual(""))
      .select($"uuid", callUDF("labelid2label", $"feature").as("label"), $"feature".as("label_id"), $"cnt", $"sum")

}
