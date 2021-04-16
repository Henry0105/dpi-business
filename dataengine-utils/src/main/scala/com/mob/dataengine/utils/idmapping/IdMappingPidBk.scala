package com.mob.dataengine.utils.idmapping

import com.mob.dataengine.commons.traits.Logging
import com.mob.dataengine.commons.utils.PropUtils._
import com.mob.dataengine.utils.DateUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import scopt.OptionParser

import scala.annotation.tailrec

case class PidBkParams(day: String = DateUtils.currentDay(), full: Boolean = false)

case class PidInfo(abnormalFlag: Int, devices: Seq[DeviceInfo])

case class DeviceInfo(device: String, time: String, cnt: Int, profileFlag: Int)

case class DeviceTime(device: String, time: String)

case class TimestampIndex(timestamp: Seq[String], index: Seq[Int])

object IdMappingPidBk {

  val deviceSep: String = "\u0001"

  def deviceIndexMap(arr: Seq[String]): Map[String, Int] = arr.indices.map(i => arr(i) -> i).toMap

  /**
   * 取出同一时间戳下唯一device
   */
  def getUniqueDevice(rows: Seq[Row])(ord: Ordering[DeviceInfo]): Seq[DeviceTime] = {
    rows.map { r =>
      //      val abnormalFlag = r.getAs[Int]("abnormal_flag")
      val deviceInfos = r.getAs[Seq[Row]]("devices").map { dr =>
        val device = dr.getAs[String]("device")
        val time = dr.getAs[String]("time")
        val cnt = dr.getAs[Int]("cnt")
        val profileFlag = dr.getAs[Int]("profile_flag")
        DeviceInfo(device, time, cnt, profileFlag)
      }
      val (hasPar, hasNotPar) = deviceInfos.partition(_.profileFlag != 0)
      val par = if (hasPar.nonEmpty) hasPar else hasNotPar
      if (par.nonEmpty) {
        val maxDevice = par.max(ord)
        DeviceTime(maxDevice.device, maxDevice.time)
      } else {
        null
      }
    }.filter(_ != null)
  }

  // 连续元素去重(左开右闭,保留最后的一个元素)
  @tailrec
  private def removeTheSameElement(xs: List[(String, Int)], ys: List[(String, Int)]): List[(String, Int)] = {
    if (xs.isEmpty) {
      return ys
    }
    if (xs.last._2 == ys.head._2) {
      removeTheSameElement(xs.init, xs.last :: ys.tail)
    } else {
      removeTheSameElement(xs.init, xs.last :: ys)
    }
  }

  /**
   * 做出 bk_tm device_index 2个数组
   * 对2个数组做连续元素去重(左闭右开,保留最左的一个元素)
   */
  def tmIndexArr(arr: Seq[Row], device2Index: Map[String, Int]): TimestampIndex = {

    val tm2device = arr.map {
      r =>
        (r.getAs[String]("time"), r.getAs[String]("device"))
    }
    val rawTm2index = tm2device.groupBy(_._1).mapValues(xs => xs.maxBy(_._2)).values.toSeq
      .sortBy(_._1)
      .map {
        case (tm, device) => (tm, device2Index(device))
      }.toList
    val tm2index = removeTheSameElement(rawTm2index.init, rawTm2index.last :: Nil)
    val tms = tm2index.map(_._1)
    val indexs = tm2index.map(_._2)
    TimestampIndex(tms, indexs)
  }

  def main(args: Array[String]): Unit = {
    println(s"初试参数:${
      args.mkString(",")
    }")
    val defaultParams: PidBkParams = PidBkParams()
    val projectName = s"IdMappingPidBk[${
      DateUtils.currentDay()
    }]"

    val parser = new OptionParser[PidBkParams](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .text("day 例如:20180806 默认当天")
        .action((x, c) => c.copy(day = x))
      opt[Boolean]('f', "full")
        .text("是否是全量生成 默认false")
        .action((x, c) => c.copy(full = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(p) =>
        println(s"参数为:$p")
        val spark: SparkSession = SparkSession
          .builder()
          .appName(s"${this.getClass.getSimpleName}_${p.day}")
          .config("spark.sql.orc.enableVectorizedReader", "true")
          .enableHiveSupport()
          .getOrCreate()

        IdMappingPidBk(spark, p).run()
        spark.close()
      case _ =>
        println(s"参数错误:${args.mkString(",")}")
        sys.exit(1)
    }
  }

}

/**
 * 增量选取规则：
 *   1.device有画像 profile_flag == true
 *   2.取出90天内出现次数最多的设备 cnt max
 */
case class IdMappingPidBk(@transient spark: SparkSession, p: PidBkParams) extends Logging {

  import org.apache.spark.sql.functions._
  import spark.implicits._
  import IdMappingPidBk._

  val day: String = p.day

  private val ord: Ordering[DeviceInfo] = (x: DeviceInfo, y: DeviceInfo) => {
    x.cnt - y.cnt match {
      case num if num != 0 => num
      case _ => Ordering.String.compare(x.device, y.device)
    }
  }
  private val tm_index_arr: UserDefinedFunction = udf(tmIndexArr(_: Seq[Row], _: Map[String, Int]))
  private val device_index_map: UserDefinedFunction = udf(deviceIndexMap(_: Seq[String]))
  private val get_unique_device: UserDefinedFunction = udf(getUniqueDevice(_: Seq[Row])(ord))

  private val headers = spark.table(HIVE_TABLE_DM_PID_MAPPING_BK).schema.fieldNames diff
    Array("day") mkString ","

  def getLastPartition(table: String, day: String): String = {
    val filterPars = sql(s"show partitions $table").collect().map(_.getAs[String](0).split("=")(1))
      .filter(_ <= day)
    if (filterPars.length == 0) "false" else s"day = '${filterPars.max}'"
  }


  def run(): Unit = {
    val ver = getLastPartition(HIVE_PID_DEVICE_TRACK_DF, day)
    val ver2 = if (p.full) "1 <> 1" else getLastPartition(HIVE_TABLE_DM_PID_MAPPING_BK, day)
    val diDF = sql(
      s"""
         |select pid, device_list, update_time
         |from   $HIVE_PID_DEVICE_TRACK_DF
         |where  $ver
         |   and ${if (p.full) "true" else s"update_time = '$day'"}
         |   and device_list is not null
         |   and cardinality(device_list) > 0
         |""".stripMargin)

    // incr表的格式化
    val incrTb = "incr_tb"
    val df = diDF.select($"pid",
      map_values($"device_list").as("device_times"), $"update_time")
      // 取出每日唯一device
      .withColumn("device_times", get_unique_device($"device_times"))
      .where(size($"device_times") > 0)
      // 取出去重的device数组
      .withColumn("device", array_distinct($"device_times.device"))
      // Map(device -> 数组下标)
      .withColumn("device_index_map", device_index_map($"device"))
      // 做出时间戳的排序数组和下标的排序数组
      .withColumn("tm_index", tm_index_arr($"device_times", $"device_index_map"))
      .selectExpr("pid", "tm_index.timestamp as bk_tm", "tm_index.index as device_index",
        "device", "update_time")
    df.createOrReplaceTempView(incrTb)

    val resTb = "res_tb"
    sql(
      s"""
         |select $headers
         |from (
         |      select $headers
         |           , row_number() over(partition by pid order by update_time desc) as rn
         |      from (
         |            select $headers
         |            from   $incrTb
         |
         |            union all
         |
         |            select $headers
         |            from   $HIVE_TABLE_DM_PID_MAPPING_BK
         |            where  $ver2
         |            ) as t1
         |      ) as t2
         |where rn = 1
         |""".stripMargin
    ).createOrReplaceTempView(resTb)

    sql(
      s"""
         |insert overwrite table $HIVE_TABLE_DM_PID_MAPPING_BK partition (day = '$day')
         |select $headers
         |from   $resTb
         |""".stripMargin)

  }

}
