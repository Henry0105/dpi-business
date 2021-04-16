package com.mob.dataengine.engine.core.crowd

import java.time.YearMonth
import java.util.UUID

import com.mob.dataengine.commons.DeviceSrcReader
import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.commons.enums.{DeviceType, EncryptType, InputType, JobName}
import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.pojo.OutCnt
import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}
import com.mob.dataengine.commons.utils.{AppUtils, PropUtils}
import com.mob.dataengine.engine.core.crowd.helper.CrowdAppTimeFilterTrans
import com.mob.dataengine.engine.core.jobsparam.{BaseJob2, CrowdAppTimeFilterInputRule, CrowdAppTimeFilterParam, JobContext2}
import com.mob.dataengine.engine.core.mapping.dataprocessor.DataEncryptionDecodingLaunchV2.mergeFeature
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.HadoopUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

@author("xlmeng")
@createTime("2020-10-26")
object CrowdAppTimeFilter extends BaseJob2[CrowdAppTimeFilterParam] {

  /* --------------------- override methods --------------------- */
  override def cacheInput: Boolean = true

  val outCnt = new OutCnt
  var lastPar: String = _
  var id2Key: String = _

  override def run(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    implicit val spark: SparkSession = ctx.spark
    println(ctx)
    val buildDF = buildInputDataFrame(ctx)
    if (cacheInput) buildDF.cache()
    ctx.matchInfo.idCnt = buildDF.count()

    val transDF = transformData(buildDF, ctx)
    transDF.persist(StorageLevel.MEMORY_AND_DISK)

    val seedDF = transformSeedDF(transDF, ctx)
    write2HDFS(seedDF, ctx)
    ctx.matchInfo.setMatchCnt(transDF.count())

    val transDF_ = transDF.repartition(getCoalesceNum(ctx.matchInfo.matchCnt, 100000))
    persist2Hive(transDF_, ctx)

    sendRPC(ctx)
  }

  def setCheckPoint(ctx: JobContext2[CrowdAppTimeFilterParam]) {
    val checkpointPath =
      s"${AppUtils.DATAENGINE_HDFS_TMP}/${ctx.jobCommon.jobName}/${DateUtils.getCurrentDay()}_${UUID.randomUUID()}"
    println("checkpoint_path:" + checkpointPath)
    ctx.spark.sparkContext.setCheckpointDir(checkpointPath)
    HadoopUtils.broadcastHadoopConfiguration(ctx.spark)
    HadoopUtils.addShutdownHook(() => {
      val fs = FileSystem.get(ctx.spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(checkpointPath)
      if (fs.exists(outPutPath)) {
        fs.delete(outPutPath, true)
      }
    })
  }

  override def buildInputDataFrame(ctx: JobContext2[CrowdAppTimeFilterParam]): DataFrame = {
    ctx.param.inputs.head.rules.get.foreach(x => {
      timeIntervalCheck(ctx, x)
    })
    import ctx.spark.implicits._
    import org.apache.spark.sql.functions._
    val inputDF = if (InputType.nonEmpty(ctx.param.inputs.head.inputTypeEnum)) {
      getInputDF(ctx)
        .select($"id".as("device"), $"data", monotonically_increasing_id().as("rowId"))
    } else {
      ctx.spark.emptyDataFrame
    }
    inputDF
  }

  // 数据的处理及其筛选
  override def transformData(df: DataFrame, ctx: JobContext2[CrowdAppTimeFilterParam]): DataFrame = {
    df.createOrReplaceTempView("input")
    getAppTable(ctx)
    ctx.param.inputs.head.rules.get.foreach(x => {
      val tableId = ctx.param.inputs.head.rules.get.indexOf(x) + 1
      CrowdAppTimeFilterTrans(ctx, x, tableId).execute(df)
    })
    getRuleJoinFilterData(ctx)
  }

  /** 读取app状态表 */
  def getAppTable(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    ctx.sql(
      s"""
         |cache table t1
         |SELECT device, pkg, app_name
         |     , installed, active, uninstall, new_install
         |     , upper_time
         |FROM ${PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll}
         |WHERE day = '$lastPar'
       """.stripMargin)
  }

  def getRuleJoinFilterData(ctx: JobContext2[CrowdAppTimeFilterParam]): DataFrame = {
    val initSql =
      s"""
         |select
         |    a1.device,a1.data,a1.rowId
         |from t4_r_1 a1
       """.stripMargin
    val strBuilder = new StringBuilder(initSql)
    val basicsJoinSql = "\r\njoin t4_r_{position} as a{position}" +
      " on a1.device = a{position}.device and a1.data = a{position}.data and a1.rowId = a{position}.rowId"
    for (x <- 2 to ctx.param.inputs.head.rules.get.size) {
      strBuilder.append(basicsJoinSql.replace("{position}", String.valueOf(x)))
    }
    ctx.sql(strBuilder.toString()).withColumnRenamed("device", ctx.param.fieldName)
  }

  override def createMatchIdsCol(ctx: JobContext2[CrowdAppTimeFilterParam]): Seq[Column] = null

  override def persist2Hive(df: DataFrame, ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    persist2DataHub(df, ctx)
    val df2 = ctx.sql(
      s"""
         |SELECT feature['$id2Key'][0] as ${ctx.param.fieldName}
         |FROM ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '${ctx.param.output.uuid}'
         |""".stripMargin)
    persist2Cache(df2, ctx)
  }

  /* --------------------- self methods --------------------- */
  def timeIntervalCheck(ctx: JobContext2[CrowdAppTimeFilterParam], rule: CrowdAppTimeFilterInputRule): Unit = {
    lastPar = ctx.getLastPar(PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll).split("=")(1)

    val (accStartTime, accEndTime) = {
      val lastDate = DateUtils.getDate(lastPar)
      val (y, m, _) = DateUtils.getYearMonthDay(lastPar)
      (DateUtils.fmt.format(lastDate.minusYears(1).withDayOfMonth(1)),
        DateUtils.fmt.format(YearMonth.of(y, m).atEndOfMonth()))
    }

    val (startTime, endTime) = rule.timeIntervalRule
    rule.timeIntervalRule = (
      if (startTime >= accStartTime) startTime else accStartTime,
      if (endTime <= accEndTime) endTime else accEndTime)

    val msg = s"today=${DateUtils.getCurrentDay()}, lastPar=$lastPar" +
      s", timeInterval=[$startTime, $endTime], " +
      s"accept timeInterval=[$accStartTime, $accEndTime]"

    //    assert(startTime >= accStartTime && endTime <= accEndTime, s"ERROR: 时间区间超出范围,$msg")
    if (startTime >= accStartTime && endTime <= accEndTime) {
      println(s"INFO 时间区间信息: $msg")
    } else {
      println(s"WARN 时间区间信息: $msg, final " +
        s"timeInterval=[${rule.timeIntervalRule._1}" +
        s", ${rule.timeIntervalRule._2}] ")
    }

  }

  def getInputDF(ctx: JobContext2[CrowdAppTimeFilterParam]): DataFrame = {
    val hubService: DataHubService = DataHubServiceImpl(ctx.spark)
    val inputDF = ctx.param.inputs.map(input => DeviceSrcReader
      .toDFV2(ctx.spark, input, None, None, isBk = false, hubService))
      .reduce(_ union _)
    inputDF
  }

  def persist2Cache(df: DataFrame, ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    val biz = s"${JobName.getId(ctx.jobCommon.jobName)}|${DeviceType.DEVICE.id}"
    import ctx.spark._
    if (ctx.matchInfo.matchCnt != 0L) {
      sql(
        s"""
           |ALTER TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
           |DROP IF EXISTS PARTITION(uuid='${ctx.param.output.uuid}', biz='$biz')
       """.stripMargin)

      df.createOrReplaceTempView("tmp")

      sql(
        s"""
           |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
           |PARTITION(created_day=${ctx.jobCommon.day},biz='$biz',uuid='${ctx.param.output.uuid}')
           |SELECT ${ctx.param.fieldName} FROM tmp
           """.stripMargin
      )
    }
  }

  def persist2DataHub(df: DataFrame, ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    import ctx.{hubService, param}
    id2Key = hubService.deduceIndex(param.inputIdTypeEnum.id.toString, EncryptType.NONENCRYPT.id.toString)
    val resultDF = df.select(map(
      lit("seed"), split(col("data"), param.sep.getOrElse("\u0001")),
      lit(id2Key), array(col(ctx.param.fieldName))).as("feature"),
      col("data")
    )
    val seedDF = readFromSeedHub(ctx)
    val mergeFeatureUDF = udf(mergeFeature(_: Map[String, Seq[String]], _: Map[String, Seq[String]]))
    val featureDF = resultDF.join(seedDF, resultDF("data") === seedDF("seed"), "left")
      .select(mergeFeatureUDF(resultDF("feature"), seedDF("feature")).as("feature"))
    hubService.writeToHub(featureDF, param.output.uuid)
  }

  def readFromSeedHub(ctx: JobContext2[CrowdAppTimeFilterParam]): DataFrame = {
    import ctx.spark.implicits._
    ctx.param.inputs.map { input =>
      ctx.sql(
        s"""
           |select feature
           |from ${PropUtils.HIVE_TABLE_DATA_HUB}
           |where uuid = '${input.uuid}'
          """.stripMargin)
    }.reduce(_ union _)
      .select($"feature".getField("seed").getItem(0).as("seed"), $"feature")
      .withColumn("rn", row_number().over(Window.partitionBy($"seed").orderBy($"seed")))
      .where($"rn" === 1)
      .select("feature", "seed")
  }

}