package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/**
 * 金融标签计算抽象类, 含标签:
 * F0001-F0009, F1001-F1035
 *
 * Sum计算逻辑: 各单体金融标签数量加总
 * Cnt计算逻辑: 去重计数单体金融标签数量大于0的设备
 *
 * @see AbstractJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
abstract class AbstractFinancialJob(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractJob(jobContext, sourceData) {

  import spark._
  import spark.implicits._

  @transient private[this] val logger = Logger.getLogger(this.getClass)

  /* 取最后分区 */
  private lazy val lastPar: String =
    sql(s"show partitions $tableName").collect().map(_.getAs[String](0)).last

  override protected lazy val originalDFLoadSQL =
    s"select device,${fields.mkString(",")} from $tableName where $lastPar"

  /* 字段名称标签映射, 例: "sum(borrowing)"->"F1001" */
  private val fieldsLabelMapping: Map[String, String] = if (needCal) {
    val map = lhtOpt.get.flatMap(_.elements.map(lhtf => s"sum(${lhtf.field})" -> lhtf.label)).toMap
    logger.info(s"fieldsLabelMapping=>[$map]")
    map
  } else Map.empty[String, String]

  @transient override protected lazy val originalDF: DataFrame = loadOriginalDF
  @transient override protected lazy val groupedDF: DataFrame =
    joinedDF.groupBy("uuid").agg(fields.map(_ -> "sum").toMap)
  @transient protected lazy val cntGroupedDF: DataFrame =
    joinedDF.groupBy("uuid", fields.toSeq: _*).agg(countDistinct("device").as("cnt")).cache()
  @transient override protected lazy val joinedDF: DataFrame = loadJoinedDF.cache()
  @transient override protected lazy val finalDF: DataFrame = {
    val otherColNames: Seq[String] = groupedDF.schema.fieldNames.filterNot(_.equals("uuid")).toSeq
    /* 计算sum */
    groupedDF.flatMap(
      r => {
        val uuid = r.getAs[String]("uuid")
        otherColNames.map(
          f => (uuid, fieldsLabelMapping(f), f.substring(4, f.length - 1), 0.0, r.getAs(f).toString.toDouble)
        )
      }
    ).toDF("uuid", "label", "label_id", "cnt", "sum")
      /* 计算cnt(distinct) */
      .union(fields.map {
      f =>
        cntGroupedDF
          .select($"uuid", col(f), $"cnt").filter(col(f).isNotNull && col(f) > 0)
          .groupBy("uuid").agg(sum("cnt").as("cnt"))
          .select($"uuid",
            lit(fieldsLabelMapping(s"sum($f)")).as("label"),
            lit(f).as("label_id"), $"cnt".cast(DataTypes.DoubleType),
            lit(0).cast(DataTypes.DoubleType).as("sum"))
    }.reduce(_ union _)).coalesce(SPARK_SQL_SHUFFLE_PARTITIONS)
      .groupBy("uuid", "label", "label_id")
      .agg(max("cnt").as("cnt"), max("sum").as("sum"))
      .select("uuid", "label", "label_id", "cnt", "sum")
  }

  override protected def clear(moduleName: String): Unit = {
    joinedDF.unpersist()
    cntGroupedDF.unpersist()
    logger.info(s"$moduleName|clear succeeded...")
  }
}
