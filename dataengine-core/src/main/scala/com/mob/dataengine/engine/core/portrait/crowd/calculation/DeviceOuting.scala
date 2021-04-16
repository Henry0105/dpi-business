package com.mob.dataengine.engine.core.portrait.crowd.calculation
import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
 * 基础标签计算逻辑, 含标签:
 * D038-D039
 * rp_mobdi_app.rp_device_outing|分区1,413,654|32.8M
 *
 * Sum计算逻辑: 非去重设备计数
 * Cnt计算逻辑: 去重设备计数
 *
 * @see AbstractJob
 */
@author("yunlong sun")
@createTime("2018-08-04")
@sourceTable("rp_mobdi_app.rp_device_outing")
case class DeviceOuting(jc: JobContext, sourceData: Option[SourceData])
  extends AbstractJob(jc, sourceData) {
  private[this] lazy val logger: Logger = Logger.getLogger(this.getClass)
  override protected lazy val tableName: String = PropUtils.HIVE_TABLE_RP_DEVICE_OUTING

  import org.apache.spark.sql.functions._
  import spark._
  import spark.implicits._

  private[this] lazy val lastPar =
    sql(s"show partitions $tableName").collect().map(_.getAs[String](0)).last.split("/")(0).split("=")(1)
  /* 取最后分区数据进行计算 */
  override protected lazy val originalDFLoadSQL: String =
    s"select device, ${fields.mkString(",")} from $tableName where day='$lastPar'"
  override protected val originalDF: DataFrame = loadOriginalDF
  override protected val joinedDF: DataFrame = loadJoinedDF.cache()
  override protected val groupedDF: DataFrame = emptyDataFrame
  override protected val finalDF: DataFrame = {
    val arr = mutable.ArrayBuffer[DataFrame]()
    calOutCountry().map(arr += _)
    calTotalTimes().map(arr += _)
    if (arr.nonEmpty) arr.reduce(_ union _) else emptyDataFrame
  }

  override protected def clear(moduleName: String): Unit = {
    joinedDF.unpersist()
    logger.info(s"$moduleName|clear succeeded...")
  }

  /* 计算出境国家: D038 */
  private[this] def calOutCountry(): Option[DataFrame] = {
    val f = "out_country"
    if (fields.contains(f)) {
      Some(
        joinedDF.select("uuid", "device", f)
          .flatMap {
            case Row(u: String, d: String, ocs: Seq[String]) =>
              ocs.map(oc => (u, d, oc)).toIterator
          }.toDF("uuid", "device", "out_country")
          .groupBy("uuid", "out_country")
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("D038").as("label"), $"out_country".as("label_id"), $"cnt", $"sum")
      )
    } else None
  }

  /* 计算出境次数: D039 */
  private[this] def calTotalTimes(): Option[DataFrame] = {
    val f = "tot_times"
    if (fields.contains(f)) {
      Some(
        joinedDF.select("uuid", "device", f)
          .groupBy("uuid", f)
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("D039").as("label"), col(f).as("label_id"), $"cnt", $"sum")
      )
    } else None
  }

  override protected lazy val _moduleName = "DeviceOuting"
}
