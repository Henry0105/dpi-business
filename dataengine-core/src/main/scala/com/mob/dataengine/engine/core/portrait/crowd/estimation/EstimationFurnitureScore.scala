package com.mob.dataengine.engine.core.portrait.crowd.estimation

import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.{CrowdPortraitEstimationParam, JobContext}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable

/**
 * 垂直画像计算-家居画像(CP001)
 * 注: 由于输入设备为deviceId, 去除原家居画像中根据adr数量换算ios数量及总数的过程
 *
 * @param jobContext JobContext任务上下文
 * @param sourceData 输入数据及布隆过滤器广播
 */
@author("yunlong sun")
@createTime("2018-07-24")
case class EstimationFurnitureScore(jobContext: JobContext[CrowdPortraitEstimationParam],
  sourceData: Option[SourceData])
  extends AbstractJob(jobContext: JobContext[CrowdPortraitEstimationParam], sourceData) {
  @transient private[this] val logger: Logger = Logger.getLogger(this.getClass)
  /* 家居画像计算对应标签, 与入参对应 */
  override protected lazy val labelName: String = "CP001"
  /* 家居画像计算所需基础标签, 全部 */
  private[this] val fields = Set("gender", "agebin", "income", "edu", "kids")
  /* 输入数据匹配后各用户device去重计数, 来源于DeviceIdTagsMapping模块 */
  private[this] val uuid2Count = sourceData.get.uuid2Count
  /* 基础标签对应编码, 与映射表匹配时使用 */
  private val label2type = Map("gender" -> 1, "agebin" -> 2, "income" -> 3, "edu" -> 4, "kids" -> 5)
  /* 结果表 */
  override lazy val tableName: String = PropUtils.HIVE_TABLE_CROWD_PORTRAIT_ESTIMATION_SCORE
  private[this] val (homeImprovementCoefficientMapping, megaCategoryIterator):
    (Broadcast[Map[Int, Iterable[(Int, Int, Double, Double)]]], immutable.Iterable[(Int, Int, Double, Double)]) =
    if (needCal) loadHomeImprovementCoefficient()
    else {
      (null, null)
    }

  import org.apache.spark.sql.functions._
  import jobContext.spark._
  import jobContext.spark.implicits._
  import jobContext.spark.sparkContext.broadcast

  @transient override protected lazy val joinedDF: DataFrame = emptyDataFrame
  /* 过滤家居画像计算所需字段 */
  @transient override protected lazy val originalDF: DataFrame = {
    if (needCal) {
      val df = sourceDF.select("uuid", (fields + "device").toSeq: _*)
        .groupBy("uuid", fields.toSeq: _*)
        .agg(count("device").as("cnt")).cache()
      df
    } else emptyDataFrame
  }
  /* 分标签聚合并组合 */
  @transient override protected lazy val groupedDF: DataFrame = {
    if (needCal) {
      fields.map {
        f =>
          originalDF.select("uuid", f, "cnt")
            .filter(col(f).isNotNull && trim(col(f)).notEqual(""))
            .groupBy($"uuid", col(f))
            .agg(sum("cnt").as("cnt"))
            .select($"uuid", lit(f).as("label"), col(f).as("label_id"), $"cnt")
      }.reduce(_ union _)
        .filter(!$"label_id".isin(Seq("other", "-1", "-2", "OTHER", "其他", "未知", "unknown", "UNKNOWN", "", -1): _*))
        .select($"uuid", $"label", $"label_id", $"cnt",
          ($"cnt".cast(DataTypes.DoubleType) / sum("cnt").over(Window.partitionBy($"uuid", $"label"))).as("percent"))
    } else emptyDataFrame
  }
  /* 结果数据 */
  @transient override protected lazy val finalDF: DataFrame = {
    if (needCal) {
      // groupedDF.show(false)
      groupedDF.flatMap {
        case Row(uuid: String, label: String, labelId: Int, cnt: Long, percent: Double) =>
          val _type = label2type(label)
//          println(s"now print _type=>${_type} labelId => $labelId newkey=> ${zip2(_type, labelId)} for debug " +
//            s"homeImprovementCoefficientMapping => ${homeImprovementCoefficientMapping.value}")
          homeImprovementCoefficientMapping.value(zip2(_type, labelId)).map {
            case (t, st, ptc, pcc) =>
              (uuid, labelName, t, st, cnt, percent * ptc * pcc)
          }
      }.toDF("uuid", "label", "type", "sub_type", "cnt", "percent")
        .groupBy("uuid", "label", "type", "sub_type")
        .agg(sum("cnt").as("cnt"), sum("percent").as("percent"))
        .union(calBrand())
        .union(calCategory())
    } else emptyDataFrame
  }

  override protected def clear(moduleName: String): Unit = {
    originalDF.unpersist()
    logger.info(s"$moduleName|clear succeeded...")
  }

  /* 加载家居类别映射系数表并广播 */
  private def loadHomeImprovementCoefficient(): (
    Broadcast[Map[Int, Iterable[(Int, Int, Double, Double)]]], immutable.Iterable[(Int, Int, Double, Double)]
    ) = {
    if (needCal) {
      val homeImprovementCoefficientMap = table(PropUtils.HIVE_TABLE_HOME_IMPROVEMENT_COEFFICIENT)
        .select(
          $"type",
          $"sub_type",
          $"property_type".as("pt"),
          $"property_code".as("pc"),
          $"property_coefficient".as("ptc"),
          $"property_code_coefficient".as("pcc")
        ).rdd.map {
        case Row(t: Int, st: Int, pt: Int, pc: Int, ptc: Double, pcc: Double) =>
          zip2(pt, pc) -> (t, st, ptc, pcc)
      }.groupByKey().collect().toMap
      /* 将大类别系数单独取出进行计算 */
      val categoryIt = homeImprovementCoefficientMap.filter(_._1 == 0).flatMap(_._2)
      broadcast(homeImprovementCoefficientMap) -> categoryIt
    } else {
      null: (Broadcast[Map[Int, Iterable[(Int, Int, Double, Double)]]], immutable.Iterable[(Int, Int, Double, Double)])
    }
  }

  /**
   * 计算家居品牌, 取映射表中的max/min中间值
   * (其中随机数根据各用户匹配设备数量计算)
   *
   * @return 家居品牌偏好结果
   */
  private def calBrand(): DataFrame = {
    table(PropUtils.HIVE_TABLE_HOME_IMPROVEMENT_BRAND)
      .select("brand", "type", "min", "max")
      .flatMap {
        case Row(brand: String, _type: Int, min: Double, max: Double) =>
          customUuids.map {
            uuid =>
              val count: Long = uuid2Count.value.getOrElse(uuid, 1000L)
              val diff = max - min
              val percent: Double = if (diff == 0) min else {
                if (count % diff == 0) (1 + min) / 100.0 else (count % diff + min) / 100.0
              }
              (uuid, labelName, _type + 4, brand, 0, percent)
          }
      }.toDF("uuid", "label", "type", "sub_type", "cnt", "percent")
  }

  /**
   * 大类别计算, 取映射表中的max/min中间值
   * (其中随机数根据各用户匹配设备数量计算, 考虑到结果间差异性, 先放大10000倍再缩小)
   *
   * @return 家居大类别偏好结果
   */
  private def calCategory(): DataFrame = {
    jobContext.spark.sparkContext.parallelize(
      megaCategoryIterator
        .flatMap {
          case (_type, subType, min, max) =>
            customUuids.map {
              uuid => {
                val _min = (min * 10000).toLong
                val _max = (max * 10000).toLong
                val count: Long = uuid2Count.value.getOrElse(uuid, 1000L)
                val diff = _max - _min // 600
                val percent = if (diff == 0) {
                  _min / 10000.0
                } else {
                  val factor = (count % diff).toInt
                  if (factor == 0) (1 + _min) / 10000.0 + 1 / 100000.0
                  else (factor + _min) / 10000.0 + factor / 100000.0
                }
                (uuid, labelName, _type, subType, 0, percent)
              }
            }
        }.toSeq).toDF("uuid", "label", "type", "sub_type", "cnt", "percent")
  }

  /* 进行编码, type<6, subType<10 */
  private def zip2(_type: Int, subType: Int): Int = {
    (_type << 4) + subType
  }
}
