package com.mob.dataengine.engine.core.portrait.crowd.estimation

import com.mob.dataengine.commons.utils.DateUtils
import com.mob.dataengine.engine.core.jobsparam.{CrowdPortraitEstimationParam, JobContext}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * 任务计算逻辑抽象根类, 含标签:
 * 家居画像: CP001
 * 服装画像: CP002
 *
 * @param jobContext JobContext任务上下文
 * @param sourceData 输入数据及布隆过滤器广播
 */
abstract class AbstractJob(jobContext: JobContext[CrowdPortraitEstimationParam], sourceData: Option[SourceData])
  extends Serializable {
  @transient private[this] val logger = Logger.getLogger(this.getClass)
  /** 任务所需计算全部画像 */
  protected val labels: Set[String] = jobContext.params.flatMap(_.inputs.flatMap(_.tagList)).toSet
  /* 全部用户列表 */
  protected val uuids: Set[String] = jobContext.params.map(_.output.uuid).toSet
  /* 结果表 */
  protected val tableName: String
  /* 模块计算Label名称: 家居画像->CP001, 服装画像->CP002 */
  protected val labelName: String
  /* 分区数量 = 600 */
  protected val SPARK_SQL_SHUFFLE_PARTITIONS = 600

  /* 各任务计算的用户组 */
  protected val customUuids: Set[String] = jobContext.params
    .filter(p => p.inputs.flatMap(_.tagList).contains(labelName))
    .map(_.output.uuid).toSet

  import jobContext.spark._
  import jobContext.spark.implicits._

  /* 家居&服装画像计算所需全部基础画像标签字段 */
  protected val totalFields: Seq[String] =
    Seq("gender", "agebin", "income", "edu", "kids", "province_cn", "city_cn", "industry", " applist")
  /* 模块级别计算Flag */
  protected val needCal: Boolean = {
    logger.info(labels)
    labels.contains(labelName)
  }

  protected val originalDF: DataFrame
  /* 源数据, 根据输入的device映射uuid, 并匹配基础画像对应标签 */
  @transient protected val sourceDF: DataFrame = {
    logger.info(s"filter sourceDF|$tableName|${customUuids.mkString(",")}")
    if (sourceData.isDefined) sourceData.get.sourceDF.filter($"uuid".isin(customUuids.toSeq: _*))
    else emptyDataFrame
  }
  protected val joinedDF: DataFrame // todo 父类如果需要使用才有定义的必要 remove
  /* 预聚合结果, 某些场景例如单行存在多标签时, 预聚合减少中间结果数据量 */
  protected val groupedDF: DataFrame// todo 父类如果需要使用才有定义的必要 remove
  /* 计算结果, 具体计算逻辑, 由子类实现 */
  protected val finalDF: DataFrame

  /* 任务提交入口 todo MDML-25-RESOLVED 与cxt.job.jobName有歧义,最好用换一个变量名如taskName */
  def submit(moduleName: String): Unit = {
    logger.info(s"${DateUtils.currentTime()}|[$moduleName]|模块计算开始")
    logger.info(s"$moduleName|${if (needCal) labelName else "pass"}")

    Try {
      /* 执行计算 */
      cal(moduleName: String)
    } match {
      /* 计算成功时, 任务成功数+1, 返回结果为SourceData */
      case Success(_) =>
      /* 计算失败时, 任务失败数+1, 返回None */
      case Failure(ex) =>
        new InterruptedException(ex.getMessage)
    }
    /* 执行清理 */
    if (needCal) clear(moduleName)
  }

  /* 计算过程, 含表创建和最终结果持久化 */
  protected def cal(moduleName: String): Option[SourceData] = {
    if (needCal) {
      persist(finalDF, 1, tableName, "partition (uuid) select label, type, sub_type, cnt, percent, uuid", "into")
      Some(SourceData(emptyDataFrame, sourceData.get.uuid2Count, sourceData.get.deviceBF))
    } else None
  }

  /* 缓存清理 */
  protected def clear(moduleName: String): Unit


  /**
   * TODO yunlong 这个方法写的太复杂, 提出来意义不大, 抽象类一般 定义好行为模板, 具体子类实现
   *
   * 结果持久化到Hive表
   *
   * @param source     结果数据
   * @param partitions 分区数量
   * @param tableName  结果表名称
   * @param hqlPart    执行语句(部分)
   * @param mode       输出模式(overwrite/into)
   */
  protected def persist(
    source: DataFrame,
    partitions: Int,
    tableName: String,
    hqlPart: String,
    mode: String): Unit = {
    logger.info(s"insert ${mode}目标表=>$tableName with partitions=$partitions")
    val tempView = s"${tableName.split("\\.")(1)}_tmp_before_insert"
    /* 聚合结果数量较少, 基本设置的分区数量为1 */
    source.repartition(partitions).createOrReplaceTempView(tempView)
    sql(s"insert $mode table $tableName $hqlPart from ${tableName.split("\\.")(1)}_tmp_before_insert")
  }
}
