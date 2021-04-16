package com.mob.dataengine.engine.core.portrait.crowd.calculation

import java.text.SimpleDateFormat

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
 * 部分旅游标签计算逻辑, 含标签:
 * LA000-LE000, LG000-LI000, LK000
 * dm_sdk_master.travel_daily|分区2944万|721.3M
 *
 * @see AbstractJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("dm_sdk_master.travel_daily")
case class TravelDaily(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractJob(jobContext, sourceData) {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  import spark._
  import spark.implicits._

  override protected lazy val tableName: String = PropUtils.HIVE_TABLE_TRAVEL_DAILY
  override protected lazy val originalDFLoadSQL: String =
    s"""select device, ${fields.mkString(",")} from $tableName
       |where day>='$firstPar' and day<='$lastPar'""".stripMargin

  private lazy val lastPar: String = sql(
    s"show partitions $tableName").collect().map(_.getAs[String](0)).last.split("=")(1)

  private lazy val firstPar: String = new SimpleDateFormat("yyyyMMdd")
    .format(DateUtils.addDays(DateUtils.parseDate(lastPar, "yyyyMMdd"), -29))

  private lazy val otherFields: Set[String] =
    Set("travel_area", "country", "province_flag", "province", "city").intersect(fields)


  @transient
  override protected lazy val originalDF: DataFrame = loadOriginalDF
  @transient
  override protected lazy val joinedDF: DataFrame = loadJoinedDF.cache()
  @transient
  override protected lazy val groupedDF: DataFrame =
    if (otherFields.nonEmpty) {
      joinedDF.select((otherFields union Set("uuid", "device")).map(f => col(f)).toSeq: _*)
        .groupBy("uuid", otherFields.toSeq: _*)
        .agg(countDistinct("device").as("cnt"), count("device").as("sum")).cache()
    } else emptyDataFrame

  @transient
  override protected lazy val finalDF: DataFrame = {
    val arr = mutable.ArrayBuffer[DataFrame]()

    calOthers().map(arr += _)
    calLC000().map(arr += _)
    calLD000().map(arr += _)
    calLE000().map(arr += _)
    calLK000().map(arr += _)

    arr.reduce(_ union _)
  }

  override def clear(moduleName: String): Unit = {
    joinedDF.unpersist()
    if (otherFields.nonEmpty) groupedDF.unpersist()
    logger.info(s"$moduleName|clear succeeded...")
  }

  /**
   * 计算标签LC000
   */
  private def calLC000(): Option[DataFrame] = {
    if (labels.contains("LC000")) {
      Some(
        /* 商务人士标签或使用商务类app */
        joinedDF.select($"uuid", $"device", when($"business_flag" === 1 ||
          $"busi_app_act" === 1, "商务差旅").as("label_id"))
          /* 使用自由行类app或有车 */
          .union(joinedDF.select($"uuid", $"device", when($"travel_app_act" === 1 || $"car" === 1, "自助游")
          /* 非以上 */
          .when($"business_flag".=!=(1) && $"busi_app_act".=!=(1) &&
          $"travel_app_act".=!=(1) && $"car".=!=(1), "跟团游")
          .as("label_id")))
          .filter($"label_id".isNotNull && trim($"label_id").notEqual(""))
          .groupBy($"uuid", $"label_id")
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("LC000").as("label"), $"label_id", $"cnt", $"sum"))
    } else None
  }

  /**
   * 计算标签LD000
   */
  private def calLD000(): Option[DataFrame] = {
    if (labels.contains("LD000")) {
      Some(
        joinedDF.select("uuid", "device", "vaca_flag")
          .filter($"vaca_flag" >= 1 && $"vaca_flag" <= 4)
          .groupBy($"uuid", $"vaca_flag")
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("LD000").as("label"), $"vaca_flag".as("label_id"), $"cnt", $"sum"))
    } else None
  }

  /**
   * 计算标签LE000
   */
  private def calLE000(): Option[DataFrame] = {
    if (labels.contains("LE000")) {
      Some(
        joinedDF.select("uuid", "device", "day")
          .groupBy($"uuid", $"device")
          .agg(countDistinct("day").as("day_cnt"))
          .select($"uuid", $"device", when($"day_cnt".leq(3), 1)
            .when($"day_cnt".leq(5).and($"day_cnt".geq(3)), 2).otherwise(3).as("label_id"))
          .groupBy("uuid", "label_id")
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("LD000").as("label"), $"label_id", $"cnt", $"sum"))
    } else None
  }

  /**
   * 计算标签LK000
   */
  private def calLK000(): Option[DataFrame] = {
    if (labels.contains("LK000")) {
      Some(
        joinedDF.select($"uuid", $"device",
          /* 时间段内是否安装廉价航空app以及打开飞行类app */
          when($"cheap_flight_installed" === 1 || $"flight_active" === 1 ||
            $"country".=!=($"pcountry"), "廉价航空").as("label_id"))
          .union(joinedDF.select($"uuid", $"device",
            /* 时间段内是否安装一般航空app以及打开飞行类app */
            when($"flight_installed" === 1 || $"flight_active" === 1 ||
              $"country".=!=($"pcountry"), "一般航空").as("label_id")))
          .union(joinedDF.select($"uuid", $"device",
            /* 一个月内是否安装或者打开火车票类app */
            when($"ticket_installed" === 1 || $"ticket_active" === 1, "轨道交通").as("label_id")))
          .union(joinedDF.select($"uuid", $"device",
            /* 是否有车或者租车类app */
            when($"rentcar_installed" === 1 || $"rentcar_active" === 1, "自驾").as("label_id")))

          .filter($"label_id".isNotNull)
          .groupBy($"uuid", $"label_id")
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("LK000").as("label"), $"label_id", $"cnt", $"sum"))
    } else None
  }

  /**
   * 计算标签LA000/LB000/LG000/LH000/LI000
   */
  private def calOthers(): Option[DataFrame] = {
    val fieldsLabelMapping = Map("travel_area" -> "LA000", "country" -> "LB000",
      "province_flag" -> "LG000", "province" -> "LH000", "city" -> "LI000")

    if (otherFields.nonEmpty) {
      Some(
        otherFields.map {
          f =>
            groupedDF.select("uuid", f, "cnt", "sum")
              .filter(col(f).isNotNull && trim(col(f)).notEqual(""))
              .groupBy("uuid", f)
              .agg(sum("cnt").as("cnt"), sum("sum").as("sum"))
              .select($"uuid", lit(fieldsLabelMapping(f)).as("label"), col(f).as("label_id"), $"cnt", $"sum")
        }.reduce(_ union _))
    } else None
  }

  override protected lazy val _moduleName = "TravelDaily"
}
