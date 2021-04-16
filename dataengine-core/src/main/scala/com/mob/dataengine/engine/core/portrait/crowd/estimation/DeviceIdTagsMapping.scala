package com.mob.dataengine.engine.core.portrait.crowd.estimation


import com.mob.dataengine.commons.DeviceSrcReader
import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.{CrowdPortraitEstimationParam, JobContext}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.sketch.BloomFilter

import scala.util.{Failure, Success, Try}

/**
 * 垂直画像计算-数据准备, 基础画像计算
 * 含标签("gender", "agebin", "income", "edu", "kids", "province_cn", "city_cn", "industry"," applist")
 *
 * @param jobContext JobContext任务上下文
 * @param sourceData 输入数据及布隆过滤器广播
 */
@author("yunlong sun")
@createTime("2018-07-24")
case class DeviceIdTagsMapping(jobContext: JobContext[CrowdPortraitEstimationParam], sourceData: Option[SourceData])
  extends AbstractJob(jobContext: JobContext[CrowdPortraitEstimationParam], sourceData) {
  @transient private[this] val logger: Logger = Logger.getLogger(this.getClass)
  /* 清理/匹配后结果表 */
  override val tableName: String = PropUtils.HIVE_TABLE_CROWD_PORTRAIT_SOURCE_DEVCIE_PROFILE
  override protected val labelName: String = StringUtils.EMPTY

  import org.apache.spark.sql.functions._
  import jobContext.spark._
  import jobContext.spark.implicits._
  import jobContext.spark.sparkContext.broadcast

  /* 是否已计算过并持久化, 可跳过计算迅速加载源表 */
  private val isCalculated: Boolean = {
    /* 如果目标表已存在且分区包含本次需要计算的所有uuid则视为已计算 */
    jobContext.params.map(p => p.output.uuid).toSet[String].map(uuid => s"uuid=$uuid").subsetOf(
      sql(s"show partitions $tableName").collect().map(_.getAs[String](0)).toSet
    )
  }
  /* 加载输入device, 并与uuid映射 */
  @transient override protected val sourceDF: DataFrame = {
    if (isCalculated) {
      emptyDataFrame
    } else {
      logger.info(s"calculate $tableName with uuids[${jobContext.params.map(_.output.uuid)}]")
      /* 新计算, uuid和device映射 */
      // TODO if deviceRDD is too large larger than one billion? this is a long term job
      jobContext.params.flatMap(
        p => {
          p.inputs.map(input => DeviceSrcReader.toRDD2(jobContext.spark, input).map(p.output.uuid -> _))
        }
      ).reduce(_ union _)
        .repartition(SPARK_SQL_SHUFFLE_PARTITIONS)
        .toDF("uuid", "device")
    }.cache()
  }
  /* 去重后的device数量, 用于确定布隆数组长度 */
  private lazy val deviceCount: Long = if (!isCalculated) sourceDF.select($"device").distinct().count() else 0
  /* 根据源数据生成布隆过滤器数组 */
  private val deviceDF: Broadcast[BloomFilter] = {
    if (!isCalculated) {
      val deviceDF = bloomFilter()
      logger.info(s"BloomFilter calculates succeeded...")
      deviceDF
    } else null: Broadcast[BloomFilter]
  }

  /* 底层数据加载(基础画像映射表) */
  @transient override protected lazy val originalDF: DataFrame = {
    if (!isCalculated) {
      val originalDFLoadSQL = s"select device, ${totalFields.mkString(",")} " +
        s"from ${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL}"
      logger.info(s"originalDFLoadSQL=>$originalDFLoadSQL")
      filterIllegalDevice(originalDFLoadSQL, deviceDF)
    } else emptyDataFrame
  }

  @transient override protected lazy val joinedDF: DataFrame = emptyDataFrame
  @transient override protected lazy val groupedDF: DataFrame = emptyDataFrame

  /* JOIN后的 */
  @transient override protected lazy val finalDF: DataFrame = {
    if (isCalculated) {
      logger.info(s"restore $tableName with uuids[${jobContext.params.map(_.output.uuid)}]")
      /* 从目标表加载本次需要计算的所有uuid分区数据 */
      table(tableName).filter($"uuid".isin(jobContext.params.map(_.output.uuid): _*))
        .repartition(SPARK_SQL_SHUFFLE_PARTITIONS)
    } else {
      logger.info(s"calculate $tableName with uuids[${jobContext.params.map(_.output.uuid)}]")
      /* 新计算, uuid和device映射 */
      // TODO if deviceRDD is too large larger than one billion? this is a long term job
      sourceDF.printSchema()
      originalDF.printSchema()
      sourceDF.join(originalDF, sourceDF("device") === originalDF("device"), "inner").drop(sourceDF("device"))
    }.cache()
  }
  /* 最终结果中各用户的匹配设备数量 */
  private val uuid2Count: Broadcast[Map[String, Long]] = {
    val uuid2Count = finalDF.groupBy("uuid").agg(count("device").as("cnt"))
      .select("uuid", "cnt").collect().map {
      case Row(uuid: String, cnt: Long) =>
        uuid -> cnt
    }.toMap
    logger.info(s"DeviceIdTagsMapping ends with uuid2Count $uuid2Count")
    broadcast(uuid2Count)
  }

  override protected def clear(moduleName: String): Unit = {
    sourceDF.unpersist()
    logger.info(s"$moduleName|clear succeeded...")
  }

  /**
   * 前置任务计算模块: 包含表创建、数据计算和结果落地
   *
   * @param moduleName 任务名称
   * @return 输入数据->用户设备计数->布隆数组广播
   */
  override def cal(moduleName: String): Option[SourceData] = {
    val result =
      Try {
        if (!isCalculated) {
          /* 根据Device数量决定分区数量, 约百万每分区 */
          val partitions: Int = if ((deviceCount >> 20) <= 0) 1 else (deviceCount >> 20).toInt
          persist(finalDF, partitions, PropUtils.HIVE_TABLE_CROWD_PORTRAIT_SOURCE_DEVCIE_PROFILE,
            s"partition (uuid) select device, ${totalFields.mkString(",")}, uuid", "overwrite")
          clear(moduleName)
          logger.info(s"$moduleName persist succeeded...")
        }
      } match {
        case Success(_) =>
          Some(SourceData(finalDF, uuid2Count, deviceDF))
        case Failure(ex) =>
          logger.info(
            s"""
               |${ex.printStackTrace()}""".stripMargin)
          logger.info(s"$moduleName failed...")
          throw new InterruptedException(ex.getMessage)
      }
    result
  }

  /**
   * 根据输入device计算布隆数组
   *
   * @return 广播的布隆数组
   */
  private def bloomFilter(): Broadcast[BloomFilter] = {
    logger.info(s"输入设备数量=>$deviceCount")
    // 当输入设备数量为空时, 退出计算
    if (deviceCount == 0) {
      logger.error(s"empty device input [deviceCount=0], exit")
      sys.exit(1)
    }
    sample() // todo MDML-25-RESOLVED deviceCount=0 处理
    broadcast(sourceDF.select($"device").stat.bloomFilter($"device", deviceCount, 0.01))
  }

  /* 样本打印 */
  private def sample(): Unit = {
    logger.info(s"输入设备sample=>")
    sourceDF.show(false)
  }

  /**
   * 根据输入Device的布隆数组对源表进行预过滤
   *
   * @param hql      加载源表语句
   * @param deviceBF 输入Device的布隆过滤器
   * @return 底层数据DataFrame
   */
  private def filterIllegalDevice(hql: String, deviceBF: Broadcast[BloomFilter]): DataFrame = {
    sql(hql).filter(r => deviceBF.value.mightContainString(r.getAs[String]("device")))
  }
}
