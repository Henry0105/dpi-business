package com.mob.dataengine.engine.core.portrait.crowd.calculation

import java.text.SimpleDateFormat
import java.util.Date

import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

import scala.util.{Failure, Success, Try}

/**
 * 任务计算逻辑抽象根类, 含标签:
 * 基础标签: D001-D029, D034-D039
 * 金融标签: F0001-F0010, F1001-F1039
 * 餐饮标签: CA00-CI000
 * 旅游标签: LA000-LR000
 *
 * @param jobContext JobContext任务上下文
 * @param sourceData 输入数据及布隆过滤器广播
 */
abstract class AbstractJob(jobContext: JobContext, sourceData: Option[SourceData])
  extends Serializable {
  @transient private[this] val logger = Logger.getLogger(this.getClass)
  @transient protected lazy val spark: SparkSession = jobContext.spark
  private lazy val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  protected val _moduleName: String

  import spark._
  import spark.implicits._

  /* 分区数量 = 600 */
  protected val SPARK_SQL_SHUFFLE_PARTITIONS = 600
  val hubService: DataHubService = DataHubServiceImpl(spark)
  /* 底层数据表名 */
  protected val tableName: String
  /* 任务参数解析 */
  @transient
  protected val lhts: LabelHiveTables = jobContext.labelHiveTables
  /* 表级别计算Flag */
  protected val needCal: Boolean = {
    // logger.info(this)
    lhts.get(tableName).isDefined && lhts.get(tableName).get.flatMap(_.elements.map(_.module)).contains(_moduleName)
  }
  /* 本次需要计算的uuid */
  protected lazy val uuids: Seq[String] = {
    val uss = lhts.get(tableName).get
      .filter(!_.elements.map(_.module).forall(!_.equalsIgnoreCase(_moduleName))).map(_.uuid)
    lhts.get(tableName).get.filter(!_.elements.map(_.module).forall(!_.equalsIgnoreCase(_moduleName))).map(_.uuid)
  }
  /* 表级别任务参数解析 */
  @transient protected val lhtOpt: Option[Seq[LabelHiveTable]] =
  if (needCal) lhts.get(tableName) else None
  /* 所需字段 */
  protected val fields: Set[String] =
    if (lhtOpt.isDefined) {
      val f = lhtOpt.get.flatMap(_.fields).toSet
      logger.info(s"$tableName|fields|${f.mkString("|")}")
      f
    } else Set.empty[String]
  /* 与表对应的全部标签 */
  protected val labels: Set[String] =
    if (lhtOpt.isDefined) lhtOpt.get.flatMap(_.elements.map(_.label)).toSet
    else Set.empty[String]

  /* 任务状态 */
  private lazy val js: JobStatus = jobContext.jobStatus

  /* 底层数据表加载语句 */
  protected val originalDFLoadSQL: String


  /* 底层数据 */
  @transient protected def loadOriginalDF: DataFrame = {
    if (needCal && originalDFLoadSQL.length > 0 ) {
      logger.info(s"original df load sql $originalDFLoadSQL")
      filterIllegalDevice(originalDFLoadSQL, sourceData.get.deviceBF)
    } else emptyDataFrame
  }

  protected val originalDF: DataFrame
  /* 源数据, 输入的uuid和device的映射 */
  @transient protected val sourceDF: DataFrame =
  if (sourceData.isDefined) sourceData.get.sourceDF else emptyDataFrame

  /* Join后中间结果 */
//  @transient protected def loadJoinedDF: DataFrame = {
//    if (needCal) {
//      val joinedDF = originalDF.join(sourceDF, sourceDF("device") ===
//        originalDF("device"), "inner").drop(sourceDF("device"))
//      joinedDF.printSchema()
//      joinedDF
//    } else emptyDataFrame
//  }
  @transient protected def loadJoinedDF: DataFrame = {
    if (needCal) {
      val _sourceDF = sourceDF.filter($"uuid".isin(uuids: _*))
      val joinedDF = originalDF.join(_sourceDF, _sourceDF("device") === originalDF("device"), "inner")
        .drop(_sourceDF("device"))
      logger.info("now print joinedDF schema in AbstractJob")
      joinedDF.printSchema()
      joinedDF
    } else emptyDataFrame
  }

  protected val joinedDF: DataFrame
  /* 预聚合结果, 某些场景例如单行存在多标签时, 预聚合减少中间结果数据量 */
  protected val groupedDF: DataFrame
  /* 计算结果, 具体计算逻辑, 由子类实现 */
  protected val finalDF: DataFrame

  /* 任务提交入口 todo MDML-25-RESOLVED 与cxt.job.jobName有歧义,最好用换一个变量名如taskName */
  def submit(moduleName: String): Unit = {
    var time = df.format(new Date())
    logger.info(s"$time|[$moduleName]|模块计算开始")

    val paramInfo =
      if (needCal) s"[${fields.mkString(",")}]\t${lhtOpt.get.mkString("\t")}"
      else "pass"
    logger.info(s"$moduleName|$tableName|$paramInfo")

    Try {
      /* 执行计算 */
      cal(moduleName: String)
    } match {
      /* 计算成功时, 任务成功数+1, 返回结果为SourceData */
      case Success(_) =>
        time = df.format(new Date())
        js.addSuccess(1)
        logger.info(
          s"""
             |$time|[$moduleName]|模块计算结束
             |总:${js.needSuccess}|成功:${js.successNums}|失败:${js.failureNums}""".stripMargin)
      /* 计算失败时, 任务失败数+1, 返回None */
      case Failure(ex) =>
        time = df.format(new Date())
        js.addFailure(1)
        logger.info(
          s"""
             |$time|[$moduleName]|模块计算失败
             |总:${js.needSuccess}|成功:${js.successNums}|失败:${js.failureNums}
             |${ex.printStackTrace()}""".stripMargin)
        throw new InterruptedException(ex.getMessage)
    }
    /* 执行清理 */
    if (needCal) clear(moduleName)
    /* 当任务全部成功时, 结束任务 */
    if (js.finished) release()
  }

  /* 计算过程, 含表创建和最终结果持久化 */
  protected def cal(moduleName: String): Option[SourceData] = {
    if (needCal) {
      // create(PropUtils.HIVE_TARGET_TABLE_CROWD_PORTRAIT_CALCULATION_SCORE,
      //  "(label string, label_id string, cnt double, sum double) partitioned by (uuid string)")
      persist(finalDF, 1, PropUtils.HIVE_TABLE_CROWD_PORTRAIT_CALCULATION_SCORE,
        "partition (uuid) select label, label_id, cnt, sum, uuid", "into")
      Some(SourceData(emptyDataFrame, sourceData.get.deviceBF))
    } else None
  }

  /* 缓存清理 */
  protected def clear(moduleName: String): Unit

  /* 任务结束 */
  private def release(): Unit = {
    jobContext.release()
  }

  /**
   * 根据输入Device的布隆数组对源表进行预过滤
   *
   * @param hql      加载源表语句
   * @param deviceBF 输入Device的布隆过滤器
   * @return 底层数据DataFrame
   */
  protected def filterIllegalDevice(hql: String, deviceBF: Broadcast[BloomFilter]): DataFrame = {
    sql(hql).filter(r => deviceBF.value.mightContainString(r.getAs[String]("device")))
    // .coalesce(PropUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
  }

  /**
   * todo yunlong 见 estimation.AbstractJob.persist
   * 结果持久化到Hive表
   *
   * @param source     结果数据
   * @param partitions 分区数量
   * @param tableName  表名
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
    source.show()
    source.repartition(partitions).createOrReplaceTempView(tempView)
    sql(s"insert $mode table $tableName $hqlPart from ${tableName.split("\\.")(1)}_tmp_before_insert")
  }

}
