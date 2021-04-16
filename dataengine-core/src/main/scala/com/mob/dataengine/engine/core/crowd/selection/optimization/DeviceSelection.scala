package com.mob.dataengine.engine.core.crowd.selection.optimization

import java.util.Date

import com.mob.dataengine.commons.DeviceCacheWriter
import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import com.mob.dataengine.engine.core.jobsparam.{CrowdSelectionOptimizationParam, JobContext}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * 人群包优选: 根据[设备最近活跃时间/指定apppkg最近活跃时间/指定app类别最近活跃时间]将输入人群包筛选出指定数量
 *
 * @param jobContext JobContext任务上下文
 */
@author("yunlong sun")
@createTime("2018-08-11")
case class DeviceSelection(jobContext: JobContext[CrowdSelectionOptimizationParam], sourceData: SourceData) {
  @transient private[this] lazy val logger: Logger = Logger.getLogger(this.getClass)
  @transient private[this] lazy val spark = jobContext.spark
  /* 选择类别[none|apppkg|cate_l1_id|cate_l2_id] */
  private[this] lazy val _type = jobContext.params.head.inputs.head.option("option_type")
  /* 类别对应的具体数值, 示例: [none:""|apppkg:"cn.coupon.kfc"|cate_l1_id:"7018"|cate_l2_id:"7018_001"] */
  private[this] lazy val value = jobContext.params.head.inputs.head.option("option_value")
  /* 输入device */
  private[this] lazy val sourceDF = sourceData.sourceDF
  /* 输入device数量 */
  private[this] lazy val totalCnt = sourceData.totalCnt
  /* 取出数量 */
  private[this] lazy val limit = jobContext.params.head.output.limit
  private[this] lazy val currDate: String = DateUtils.currentDay()

  import spark._
  import spark.implicits._

  /* 获取指定[apppkg/一级分类/二级分类]对应pkg列表, 并广播 */
  private[this] val apppkgs: scala.Option[Broadcast[Seq[String]]] = {
    val apppkgArr: ArrayBuffer[Seq[String]] = ArrayBuffer[Seq[String]]()

    jobContext.params.foreach { p =>
      println(p)
      p.inputs.foreach{ p =>
        p.option("option_type").trim.toLowerCase match {
          case "none" =>
            logger.info(s"skip load apppkgs with [type=${p.option("option_type")}]")
            None
          case "apppkg" =>
            val _apppkgs: Seq[String] = p.option("option_value").split(",").toSeq

            // apppkgs为空时, 报错并退出
            if (_apppkgs.isEmpty) {
              logger.error(s"load apppkgs with [type=${p.option("option_type")}" +
                s"|value=${p.option("option_value")}] empty, exit")
              sys.exit(1)
            }

            logger.info(s"load apppkgs with [type=${p.option("option_type")}" +
              s"|value=${p.option("option_value")}] succeed," +
              s"{${_apppkgs.take(20).mkString("|")}}")

            apppkgArr += _apppkgs
          case _ =>
            val _apppkgs: Seq[String] = table(PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR)
              .filter($"version".equalTo(lastPar(PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR))
                and col(p.option("option_type")).isin(p.option("option_value").split(",").toSeq: _*))
              .select("apppkg").collect().map(_.getAs[String](0)).toSeq

            // apppkgs为空时, 报错并退出
            if (_apppkgs.isEmpty) {
              logger.error(s"load apppkgs with [type=${p.option("option_type")}" +
                s"|value=${p.option("option_value")}] empty, exit")
              sys.exit(1)
            }

            logger.info(s"load apppkgs with [type=${p.option("option_type")}" +
              s"|value=${p.option("option_value")}] succeed," +
              s"{${_apppkgs.take(20).mkString("|")}}")

            apppkgArr += _apppkgs
        }
      }
    }

    if (apppkgArr.nonEmpty) {
      Some(spark.sparkContext.broadcast(apppkgArr.reduce(_ ++ _).distinct))
    } else {
      None
    }
  }
  /* 根据选择类型加载不同源表的底层数据 */
  @transient private[this] lazy val originalDF = {
    logger.info(s"load originalDF with [type=${_type}|value=$value]")
    getOriginalDF()
  }

  def getOriginalDF(): DataFrame = {
    DeviceSelectionTypes.withName(_type.trim.toLowerCase) match {
      case DeviceSelectionTypes.NONE =>
        loadDataWithNoneType()
      case DeviceSelectionTypes.APPPKG =>
        loadDataWithOtherType()
      case DeviceSelectionTypes.CATEL1 =>
        loadDataWithOtherType()
      case DeviceSelectionTypes.CATEL2 =>
        loadDataWithOtherType()
    }
  }
  /* 执行join操作 */
  @transient private[this] lazy val joinedDF = {
    logger.info(s"join sourceDF with originalDF succeed")
    getJoinedDF()
  }

  def getJoinedDF(): DataFrame = {
    originalDF.join(sourceDF, originalDF("device") === sourceDF("device"), "inner")
      .drop(sourceDF("device"))
      .select($"device", $"ordered_col")
  }


  def getFinalDF(jobContext: JobContext[CrowdSelectionOptimizationParam]): DataFrame = {
    var dfArr: ArrayBuffer[DataFrame] = ArrayBuffer[DataFrame]()
    jobContext.params.foreach { p =>
      var partitions =
        if ((Math.ceil(totalCnt / p.output.limit.toDouble).toInt >>> 1) <= 1) 1
        else Math.ceil(totalCnt / p.output.limit.toDouble).toInt >>> 1
      var _finalDF: DataFrame = joinedDF
      if (totalCnt < 20000000 || limit < 20000000) {
        var partitions =
          if ((Math.ceil(totalCnt / limit.toDouble).toInt >>> 1) <= 1) 1
          else Math.ceil(totalCnt / limit.toDouble).toInt >>> 1

        while (partitions > 0) {
          if (partitions == 1) {
            // 最后的循环中丢弃"ordered_col"字段减少Shuffle数量
            _finalDF = _finalDF.repartition(partitions)
              .sortWithinPartitions($"ordered_col".desc)
              .mapPartitions(_.take(p.output.limit).map(_.getAs[String](0)))
              .toDF("data")
          } else {
            _finalDF = _finalDF.repartition(partitions)
              .sortWithinPartitions($"ordered_col".desc)
              .mapPartitions(_.take(p.output.limit))(RowEncoder(_finalDF.schema))
              .toDF("data", "ordered_col")
          }
          logger.info(s"transfer finalDF with [partitions=$partitions]")
          partitions = partitions >>> 1
        }
        dfArr += _finalDF
      } else {
        _finalDF = _finalDF.orderBy($"ordered_col".desc).drop("ordered_col").rdd.zipWithIndex()
          .filter { case (r, idx) => idx < limit }.keys.map(_.getString(0)).toDF("data")
        dfArr += _finalDF
      }
    }

    dfArr.reduce(_.union(_))
  }

  def submit(moduleName: String): Unit = {
    val finalDF = getFinalDF(jobContext)
    val partitions = if ((limit >>> 20) <= 0) 1 else limit >>> 20
    DeviceCacheWriter.insertTable2(spark, jobContext.jobCommon,
      jobContext.params.head.output, finalDF.repartition(partitions),
      s"${JobName.getId(jobContext.jobCommon.jobName)}|${jobContext.params.head.inputs.head.idType}")
    logger.info(s"$moduleName|persist to ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} with [" +
      s"created_day='$currDate'|" +
      s"biz='${JobName.getId(JobName.CROWD_SELECTION_OPTIMIZATION.toString)}|${DeviceType.DEVICE.id}'|" +
      s"uuid=${jobContext.params.head.output.uuid}|" +
      s"numPartitions=$partitions] succeed")
  }

  /* 根据设备最近活跃时间加载数据 */
  private[this] def loadDataWithNoneType(): DataFrame = {
    table(PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL)
      .select($"device", $"processtime_all".as("ordered_col"))
  }

  /* 根据指定apppkg最近活跃时间加载数据 */
  private[this] def loadDataWithOtherType() = {
    val _halfYearBefore = DateFormatUtils.format(
      org.apache.commons.lang3.time.DateUtils.addMonths(new Date(), -6), "yyyyMMdd")
    table(PropUtils.HIVE_TABLE_DEVICE_ACTIVE_APPLIST_FULL)
      // 取最后分区, 指定apppkg范围且半年以内的数据
      .filter(
      $"day".equalTo(lastPar(PropUtils.HIVE_TABLE_DEVICE_ACTIVE_APPLIST_FULL))
        and $"apppkg".isin(apppkgs.get.value: _*)
        and $"processtime".geq(_halfYearBefore)
    )
      .select("device", "processtime")
      .groupBy("device").agg(max("processtime").as("processtime"))
      .select($"device", $"processtime".as("ordered_col"))
  }

  /** 获取指定表的最后分区, 适用于一级分区表, 如:
   * 1. day=20180801
   * 2. version=1000.1001
   */
  private[this] def lastPar(tableName: String) = {
    sql(s"show partitions $tableName")
      .collect().map(_.getAs[String](0)).last.split("=")(1).split("\\.")(0)
  }
}
