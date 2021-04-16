package com.mob.dataengine.utils.tags

import com.mob.dataengine.commons.utils.DateUtils.currentTime2
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

abstract class TableStateManager(@transient spark: SparkSession, tableName: String) extends Serializable {
  @transient val logger: Logger = LoggerFactory.getLogger(getClass)
  private val currentTableStates = mutable.Map.empty[String, String]
  val lastTableStates: Map[String, String] = load()
  @transient val accumulators = new mutable.ListBuffer[MaxAccumulator]()

  import spark.implicits._

  // TODO 未来使用udf获取hive metadata
  val tablePartitions: Map[String, String]

  // load
  def load(): Map[String, String] = {
    val pars = spark.sql(s"show partitions $tableName").collect().map(_.getString(0))
    if (pars.nonEmpty) {
      val par = pars.max
      sql(s"select table, day from $tableName where $par")
        .collect().map { r => (r.getString(0), r.getString(1)) }.toMap
    } else {
      Map.empty[String, String]
    }
  }

  def getLastUpdateTime(table: String): Option[String] = lastTableStates.get(table)

  def getLastUpdateTimeOrElse(table: String, default: String): String = lastTableStates.getOrElse(table, default)

  // update
  def update(table: String, timestamp: String): Unit = {
    currentTableStates.put(table, timestamp)
  }

  def update(acc: AccumulatorV2[Long, Long]): Unit = {
    currentTableStates.put(acc.name.get, acc.value.toString)
  }

  // persist 任务执行完之后
  def persist(): Unit = {
    accumulators.foreach(accumulator => {
      update(accumulator)
    })

    logger.info("current table partitions stat:")
    currentTableStates.foreach { case (k, v) => logger.info(s"$k=>$v") }

    currentTableStates.toSeq
      .toDF("table", "day").coalesce(1)
      .createOrReplaceTempView("curr")

    sql("select table, day from curr").show(false)

    sql(
      s"""
         |insert overwrite table $tableName partition(version='${currentTime2()}')
         |select table, day
         |from curr
       """.stripMargin)
  }

  def sql(sqlString: String): DataFrame = {
    println("\n>>>>>>>>>>>>>>>>>")
    println(sqlString)
    val df = spark.sql(sqlString)
    println("<<<<<<<<<<<<<<\n\n")
    df
  }
}

class AndroidTableStateManager(@transient spark: SparkSession)
  extends TableStateManager(spark, PropUtils.HIVE_TABLE_DM_DEVICE_PROFILE_INFO_METADATA) {
  import spark.implicits._

  override val tablePartitions: Map[String, String] = {
    val prefix = "show partitions "
    val tables = Seq(
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_4_7",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_3_30",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_7_40",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_30",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_2_30",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_90",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_9_30",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_6_30",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_3_30",
      PropUtils.HIVE_TABLE_TRAVEL_LABEL_MONTHLY,
      PropUtils.HIVE_TABLE_DEVICE_TAG_CODIS_FULL,
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_30",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_7",
      s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_7"
    )

    val pars = tables.map { str =>
      val (table, days) = str match {
        case tb if tb.startsWith("rp_mobdi_app.timewindow_") =>
          val pattern = """rp_mobdi_app.timewindow_(.*?)_(\d+)_(\d+)""".r
          tb match {
            case pattern(suffix, flag, timwindow) =>
              val days = sql("show partitions rp_mobdi_app.timewindow_" + suffix)
                .map(_.getString(0))
                .collect()
                .filter(_.contains(s"flag=$flag/"))
                .filter(_.contains(s"timewindow=$timwindow"))
                .map(_.split("/", 3)(1).split("=", 2)(1)).toSeq // get 20180804

              (tb, days)
          }
        case tb =>
          val days = sql(prefix + tb).collect().map(_.getString(0).split("=", 2)(1)).toSeq
          (tb, days)
      }

      (table, days.max)
    }.toMap
    logger.info("show partitions : ")
    pars.foreach { case (k, v) => logger.info(s"$k=>$v") }
    pars
  }
}

class IosTableStateManager(@transient spark: SparkSession)
  extends TableStateManager(spark, PropUtils.HIVE_TABLE_DM_IDFA_PROFILE_INFO_METADATA) {
  import spark.implicits._

  override val tablePartitions: Map[String, String] = {
    val prefix = "show partitions "
    val tables = Seq(
      PropUtils.HIVE_TABLE_LOCATION_MONTHLY_IOS,
      PropUtils.HIVE_TABLE_IOS_ACTIVE_TAG_LIST,
      PropUtils.HIVE_TABLE_IOS_PERMANENT_PLACE
    )

    val pars = tables.map { str =>
      val (table, days) = str match {
        case tb if tb.startsWith("rp_mobdi_app.timewindow_") =>
          val pattern = """rp_mobdi_app.timewindow_(.*?)_(\d+)_(\d+)""".r
          tb match {
            case pattern(suffix, flag, timwindow) =>
              val days = sql("show partitions rp_mobdi_app.timewindow_" + suffix)
                .map(_.getString(0))
                .collect()
                .filter(_.contains(s"flag=$flag/"))
                .filter(_.contains(s"timewindow=$timwindow"))
                .map(_.split("/", 3)(1).split("=", 2)(1)).toSeq // get 20180804

              (tb, days)
          }
        case tb =>
          val days = sql(prefix + tb).collect().map(_.getString(0).split("=", 2)(1)).toSeq
          (tb, days)
      }

      (table, days.max)
    }.toMap
    logger.info("show partitions : ")
    pars.foreach { case (k, v) => logger.info(s"$k=>$v") }
    pars
  }
}
