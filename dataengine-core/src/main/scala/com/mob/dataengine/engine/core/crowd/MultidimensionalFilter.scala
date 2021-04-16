package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.enums.InputType.InputType
import com.mob.dataengine.commons.{DeviceCacheWriter, DeviceSrcReader, JobCommon}
import com.mob.dataengine.commons.enums.{DeviceType, InputType, JobName, OperatorType}
import com.mob.dataengine.commons.pojo.{MatchInfo, OutCnt}
import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}
import com.mob.dataengine.commons.utils.{FnHelper, PropUtils}
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, BaseJob2, JobContext2, MultidimensionalFilterOutput, MultidimensionalFilterParam}
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/*
 * 多维过滤任务
 * # author : jiandd
 * # create : 20191102
 * */
object MultidimensionalFilter extends BaseJob2[MultidimensionalFilterParam] {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  override def cacheInput: Boolean = true
  val outCnt = new OutCnt

  override def buildInputDataFrame(ctx: JobContext2[MultidimensionalFilterParam]): DataFrame = {
    val inputDF = if (InputType.nonEmpty(ctx.param.inputs.head.inputTypeEnum)) {
      getInputDF(ctx)
    } else {
      ctx.spark.emptyDataFrame
    }
    inputDF
  }

  /**
   * @param df transform dataframe
   * @param ctx 任务上下文
   * */
  override def transformData(df: DataFrame, ctx: JobContext2[MultidimensionalFilterParam]): DataFrame = {
    val getFilterUDF: UserDefinedFunction = ctx.spark.udf.register("getFilterUDF", filterClude (_, ctx.param))
    val tagsDF = getTagsDF(ctx.spark)
    val filtertagsDF = tagsDF.filter(getFilterUDF(tagsDF("tags")))
    val joinDF = if (ctx.matchInfo.idCnt <= 0) {
      filtertagsDF
    } else {
      getInnerJoinDF(df, filtertagsDF, ctx)
    }
    getFilter(joinDF, ctx)
  }

  override def persist2Hive(df: DataFrame, ctx: JobContext2[MultidimensionalFilterParam]): Unit = {
    val biz = s"${JobName.getId(ctx.jobCommon.jobName)}|${DeviceType.DEVICE.id}"

    import ctx.spark._

    if(ctx.matchInfo.matchCnt != 0L) {
      sql(
        s"""
           |ALTER TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
           |DROP IF EXISTS PARTITION(uuid='${ctx.param.output.uuid}', biz='$biz')
       """.stripMargin)

      df.createOrReplaceTempView("tmp")
      ctx.sql(
        s"""
           |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
           |PARTITION(created_day=${ctx.jobCommon.day},biz='$biz',uuid='${ctx.param.output.uuid}')
           |SELECT id FROM tmp
           """.stripMargin
      )
      // todo 写datahub表,缓存没有起效果
      // persist2DataHub(ctx)
    }
  }


  def persist2DataHub(ctx: JobContext2[MultidimensionalFilterParam]): Unit = {
    ctx.param.inputs.map{ input =>
        ctx.sql(
          s"""
             |select feature
             |from ${PropUtils.HIVE_TABLE_DATA_HUB}
             |where uuid = '${input.uuid}'
          """.stripMargin)
    }.reduce(_ union _).createOrReplaceTempView("seed_tb")

    ctx.spark.udf.register("merge_feature", (resultMap: Map[String, Seq[String]],
                                         seedMap: Map[String, Seq[String]],
                                         tags: Map[String, Seq[String]]) => {
      val tmpRes =
        if (null == resultMap || resultMap.isEmpty) {
          seedMap ++ tags
        } else if (null == seedMap || seedMap.isEmpty) {
          resultMap ++ tags
        } else {
          resultMap ++ seedMap ++ tags
        }
      tmpRes.filter{ case (_, seq) => null != seq && seq.nonEmpty}
    })

    ctx.spark.udf.register("add_backdate", (tags: Map[String, String]) => {
      tags.mapValues(f => Seq("", f))

    })

    // todo 当上游的任务数据过大时,这里的join很花时间
    /**
     * select merge_feature(map('seed', array(data)), feature, add_backdate(tags))
     * from tmp
     * left join (
     *   select seed, feature
     *   from (
     *     select feature['seed'][0] seed, feature,
     *       row_number() over (partition by feature['seed'][0] order by feature['seed'][0]) rn
     *     from seed_tb
     *   ) as a
     *   where rn = 1
     * ) as b
     * on tmp.data = b.seed
     */
    ctx.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_HUB} partition(uuid='${ctx.param.output.uuid}')
         |select map('seed', array(data))
         |from tmp
        """.stripMargin)
  }



  def getInputDF(ctx: JobContext2[MultidimensionalFilterParam]): DataFrame = {
    val hubService: DataHubService = DataHubServiceImpl(ctx.spark)
    val inputDF = ctx.param.inputs.map(input => DeviceSrcReader
      .toDFV2(ctx.spark, input, None, None, isBk = false, hubService))
      .reduce(_ union _)
    inputDF
  }

  def getTagsDF(spark: SparkSession): DataFrame = {
    spark.sql(
      s"""
         |SELECT device as id, tags, confidence
         |FROM ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_VIEW}
         |WHERE tags is not null
       """.stripMargin)
  }

  def getInnerJoinDF(inputDF: DataFrame, tagsDF: DataFrame,
                     ctx: JobContext2[MultidimensionalFilterParam]): DataFrame = {
    (if (ctx.matchInfo.idCnt > 10000000) { // 1kw
      tagsDF.join(inputDF, inputDF(ctx.param.deviceString) === tagsDF(ctx.param.deviceString), "inner")
    } else {
      tagsDF.join(broadcast(inputDF),
        inputDF(ctx.param.deviceString) === tagsDF(ctx.param.deviceString), "inner")
    }).drop(tagsDF(ctx.param.deviceString))
  }

  def filterClude(tags: Map[String, String], param: MultidimensionalFilterParam): Boolean = {
    var flag = true
    if(param.inputs.head.numContrast.nonEmpty) {
      val numContrastMap = param.inputs.head.numContrast.get
      flag = numContrastMap.forall(f => {
        val key = f._1
        val value = f._2
        val tagsValue = tags.getOrElse(key, None)

        if(tagsValue != None) {
          numContrast(value.trim, tagsValue.toString.trim)
        } else false
      })
    }
    if(param.inputs.head.include.nonEmpty) {
      val includeMap = param.inputs.head.include.get
      includeMap.foreach(f => {
        val key = f._1
        val value = f._2.split(",")
        val tagsValue = tags.getOrElse(key, None)
        if (tagsValue != None) {
          if ((tagsValue.toString.split(",").toSet & value.toSet).isEmpty) flag = false
        } else flag = false
      })
    }
    if(param.inputs.head.exclude.nonEmpty) {
      val excludeMap = param.inputs.head.exclude.get
      excludeMap.foreach(f => {
        val key = f._1
        val value = f._2.split(",")
        val tagsValue = tags.getOrElse(key, None)
        if (tagsValue != None) {
          if ((tagsValue.toString.split(",").toSet & value.toSet).nonEmpty) flag = false
        }
      })
    }
    flag
  }

  def getFilter(df: DataFrame, ctx: JobContext2[MultidimensionalFilterParam] ): DataFrame = {
    import ctx.spark.implicits._
    val filterDF = if (df.schema.fieldNames.contains("data")) {
      df.select("id", "data")
    } else {
      df.select("id").withColumn("data", $"id")
    }

    filterDF.withColumn(ctx.param.fieldName, col("id"))
  }

  def numContrast(value: String, tagsValue: String): Boolean = {
    if (value == "") {
      return value.equals(tagsValue)
    }
    val regex: Regex = "[|&]".r
    regex.findFirstIn(value) match {
      case Some(logicOperator) => OperatorType.withName(logicOperator) match {
        case OperatorType.OR =>
          val arr = value.split("\\|+")
          contrast(arr(0), tagsValue) | contrast(arr(1), tagsValue)
        case OperatorType.AND =>
          val arr = value.split("&+")
          contrast(arr(0), tagsValue) & contrast(arr(1), tagsValue)
      }
      case None => contrast(value, tagsValue)
    }


  }

  def contrast(value: String, tagsValue: String): Boolean = {
    val regex: Regex = "[^0-9.-]".r
    val sep = regex.findAllIn(value).mkString("")
    val num = value.replaceAll(sep, "").toDouble
    val numValue = tagsValue.toDouble

    OperatorType.withName(sep) match {
      case OperatorType.GREATER_OR_EQUAL => numValue >= num
      case OperatorType.GREATER => numValue > num
      case OperatorType.LESS_OR_EQUAL => numValue <= num
      case OperatorType.LESS => numValue < num
      case OperatorType.EQUAL => numValue == num
      case OperatorType.NOT_EQUAL => numValue != num
    }
  }



  /**
   * @param ctx 任务上下文
   * */
  override def createMatchIdsCol(ctx: JobContext2[MultidimensionalFilterParam]): scala.Seq[Column] = null




}


