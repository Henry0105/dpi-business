package com.mob.dataengine.utils.tags

import com.mob.dataengine.commons.traits.Logging
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.DateUtils
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import scopt.OptionParser

import scala.collection.mutable

object DmTagsV2Generator {
  case class Params(
    par: String = "",
    endDay: String = "",
    span: Int = 0
  )

  def main(args: Array[String]): Unit = {
    val defaultParams: Params = Params()
    val projectName = s"TagsHFileGenerator[${DateUtils.currentDay()}]"

    val parser = new OptionParser[Params](projectName) {
      head(s"$projectName")
      opt[String]('d', "par")
        .text("要导入全量表的分区")
        .action((x, c) => c.copy(par = x))
      opt[String]('3', "endDay")
        .text("要导入的数据的回溯结束日期")
        .action((x, c) => c.copy(endDay = x))
      opt[Int]('s', "span")
        .text("往后推几天")
        .action((x, c) => c.copy(span = x))
    }

    // 数据源目前只是回溯的数据源

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        val t = DmTagsV2Generator(spark)
        val startDay = DateUtils.getDayBefore(params.endDay, -1 * params.span)
        val df = t.buildDaysProfile(startDay, params.endDay)
        t.mergeIntoFull(df, params.par)
      case _ => sys.exit(1)
    }
  }
}

case class DmTagsV2Generator(@transient spark: SparkSession) extends Logging {
  spark.udf.register("expand2seq", expand2Seq _)
  spark.udf.register("agg_by_span", new AggTagsBySpan)

  import spark.implicits._

  def buildDaysProfile(startDay: String, endDay: String): DataFrame = {
    sql(
      s"""
         |select device, expand2seq(profile, day) profile
         |from (
         |  select device, profile, day
         |  from ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V2}
         |  where day >= '$startDay' and day <= '$endDay' and device rlike '[0-9a-f]{40,40}' and length(device) = 40
         |    and length(day) = 8
         |  union all
         |  select device, profile, day
         |  from ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY}
         |  where day >= '$startDay' and day <= '$endDay' and device rlike '[0-9a-f]{40,40}' and length(device) = 40
         |    and length(day) = 8
         |) a
       """.stripMargin)
  }

  def expand2Seq(m: Map[String, String], day: String): Map[String, Seq[String]] = {
    m.mapValues(v => Seq(day, v))
  }

  def mergeIntoFull(daysDF: DataFrame, par: String): Unit = {
    daysDF.createOrReplaceTempView("tmp")
    val lastPar = sql(s"show partitions ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_V2}")
        .map(_.getString(0)).collect().max.split("=")(1)

    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_V2} partition(day='$par')
         |select device, agg_by_span(profile) profile
         |from (
         |  select device, profile
         |  from tmp
         |  union all
         |  select device, profile
         |  from ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_V2}
         |  where day = '$lastPar'
         |) as a
         |group by device
       """.stripMargin)
  }
}

/**
 * 该UDAF是合并过的带有时间的标签再聚合
 */
class AggTagsBySpan extends UserDefinedAggregateFunction {

  //   指定输入的数据类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("agg_info", MapType(StringType, types.ArrayType(StringType)))))
  }

  //   聚合结果类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("device_tm", MapType(StringType, types.ArrayType(StringType)))))
  }

  //   返回的数据类型
  override def dataType: DataType = {
    //    StructType(StructField("m", MapType(StringType, ArrayType(StringType))) :: Nil)
    MapType(StringType, types.ArrayType(StringType))
  }

  override def deterministic: Boolean = true

  //   在聚合之前初始化结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new mutable.HashMap[String, Seq[String]]() {
      override def initialSize (): Int = 50
    }
  }

  //   map侧合并
  // profile_id -> (day, value)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var tmMap = buffer.getMap[String, Seq[String]](0)
    val aggInfo = input.getMap[String, Seq[String]](0)

    aggInfo.foreach{ case (profileId, arr) =>
      tmMap = updateTag(tmMap, profileId, arr(0), arr(1))
    }

    buffer.update(0, tmMap)
  }

  //   reduce测合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var m1 = buffer1.getMap[String, Seq[String]](0)
    val m2 = buffer2.getMap[String, Seq[String]](0)

    m2.foreach{ case (profileId, Seq(day, value)) =>
      m1 = updateTag(m1, profileId, day, value)
    }

    buffer1.update(0, m1)
  }

  def updateTag(m: scala.collection.Map[String, Seq[String]],
    profileId: String, day: String, value: String): scala.collection.Map[String, Seq[String]] = {
    if (m.contains(profileId)) {
      if (m(profileId).head < day) { // update value
        m.updated(profileId, Seq(day, value))
      } else {
        m
      }
    } else {
      m.updated(profileId, Seq(day, value))
    }
  }


  //   返回udaf最后的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getMap[String, Seq[String]](0)
  }
}
