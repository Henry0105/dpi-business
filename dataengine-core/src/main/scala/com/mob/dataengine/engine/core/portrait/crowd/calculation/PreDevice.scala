package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.DeviceSrcReader
import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.sketch.BloomFilter

import scala.util.{Failure, Success, Try}

/**
 * 前置任务:
 * 1. 解析输入数据为uuid和device映射关系并缓存
 * 2. 持久化到hive表rp_dataengine.portrait_source_device
 * 3. 根据输入device计算布隆数组用于底层数据预过滤加载以减小数据量
 */
@author("yunlong sun")
@createTime("2018-07-04")
case class PreDevice(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractJob(jobContext, sourceData) {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  import spark._
  import spark.implicits._
  import spark.sparkContext.broadcast

  override protected val tableName: String =
    PropUtils.HIVE_TABLE_DATA_OPT_CACHE

  /* 是否已计算过并持久化, 可跳过计算加载源表 */
  private val isCalculated: Boolean = {
    // create(tableName, "(device string) partitioned by (uuid string)")
    /* 如果目标表已存在且分区包含本次需要计算的所有uuid则视为已计算 */
    jobContext.uuids.map(u => s"uuid=$u") subsetOf
      sql(s"show partitions $tableName").collect().map(_.getAs[String](0)).toSet
  }
  @transient override protected val finalDF: DataFrame = {
    if (isCalculated) {
      logger.info(s"restore $tableName with uuids[${jobContext.uuids}]")
      /* 从目标表加载本次需要计算的所有uuid分区数据 */
      table(tableName).filter($"uuid".isin(jobContext.uuids.toSeq: _*))
        .repartition(SPARK_SQL_SHUFFLE_PARTITIONS)
    } else {
      logger.info(s"calculate $tableName with uuids[${jobContext.uuids}]")
      /* 新计算, uuid和device映射 */
      // TODO if deviceRDD is too large larger than one billion? this is a long term job
      jobContext.job.params.map(
        p => {
          p.inputs.map(input => {
            DeviceSrcReader.toDeviceRDD2(spark, input).map(p.output.uuid -> _)
          }).reduce(_ union _)
        }
      ).reduce(_ union _)
        .repartition(SPARK_SQL_SHUFFLE_PARTITIONS)
        .toDF("uuid", "device")
    }.cache()
  }

  /* 去重后的device数量, 用于确定布隆数组长度 */
  private lazy val totalCount: Long = finalDF.select($"device").distinct().count()
  /* 根据Device数量决定分区数量, 约百万每分区 */
  private lazy val partitions: Int = if ((totalCount >> 20) <= 0) 1 else (totalCount >> 20).toInt


  /**
   * 前置任务计算模块: 包含表创建、数据计算和结果落地
   *
   * @param moduleName 任务名称
   * @return 输入数据->布隆数组广播
   */
  override def cal(moduleName: String): Option[SourceData] = {
    val result =
      Try {
        if (!isCalculated) {
          persist(finalDF, partitions, tableName,
            s"partition (created_day='${DateUtils.currentDay()}'" +
              s",biz='${JobName.getId(jobContext.job.jobCommon.jobName)}|" +
              s"${DeviceType.DEVICE.id}',uuid) select device, uuid", "overwrite")
          logger.info(s"$moduleName persist succeeded...")
        }
      } match {
        case Success(_) =>
          val deviceDF = bloomFilter()
          logger.info(s"$moduleName bloom filter prepared succeeded...")
          Some(SourceData(finalDF, deviceDF))
        case Failure(e) =>
          e.printStackTrace()
          logger.info(s"$moduleName failed...")
          None
      }
    result
  }

  /**
   * 根据输入device计算布隆数组
   *
   * @return 广播的布隆数组
   */
  private def bloomFilter(): Broadcast[BloomFilter] = {
    logger.info(s"输入设备数量=>$totalCount")
    sample()
    broadcast(finalDF.select($"device").stat.bloomFilter($"device", totalCount, 0.01))
  }

  /* 样本打印 */
  private def sample(): Unit = {
    logger.info(s"输入设备sample=>")
    finalDF.show(false)
  }

  override protected val originalDFLoadSQL: String = ""
  override protected val originalDF: DataFrame = emptyDataFrame
  override protected val joinedDF: DataFrame = emptyDataFrame
  override protected val groupedDF: DataFrame = emptyDataFrame

  override def clear(moduleName: String): Unit = {
    logger.info(s"$moduleName|nothing to clear...")
  }

  override protected lazy val _moduleName = ""
}
