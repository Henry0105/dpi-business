package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.pojo._
import com.mob.dataengine.commons.utils.{FnHelper, PropUtils}
import com.mob.dataengine.commons.{DeviceCacheWriter, DeviceSrcReader, JobCommon}
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, CrowdFilterInput, CrowdFilterParam}
import com.mob.dataengine.rpc.RpcClient
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.broadcast

/**
 * @author juntao zhang
 */
object CrowdFilter extends BaseJob[CrowdFilterParam] {
  override def run(): Unit = {
    jobContext.params.foreach(param => {
      println(param)
      val crowdFilterJob = CrowdFilterJob(jobContext.spark, jobContext.jobCommon, param)
      val (idCnt, deviceCnt) = crowdFilterJob.submit()
      if (jobContext.jobCommon.hasRPC()) {
        sendMatchInfo(idCnt, deviceCnt, crowdFilterJob, jobContext.jobCommon, param)
      }
    })
  }

  def sendMatchInfo(idCnt: Long, total: Long, crowdFilterJob: CrowdFilterJob,
    job: JobCommon, p: CrowdFilterParam): Unit = {
    val outCnt = new OutCnt
    outCnt.set("device_cnt", total)
    val matchInfo = MatchInfo(job.jobId, p.output.uuid, idCnt, total, outCnt)
    if (job.hasRPC()) {
      RpcClient.send(job.rpcHost, job.rpcPort, s"2\u0001${p.output.uuid}\u0002${matchInfo.toJsonString()}")
    } else {
      println(matchInfo.toJsonString())
    }
  }
}

case class CrowdFilterJob(spark: SparkSession, jobCommon: JobCommon, param: CrowdFilterParam) {
  import spark.implicits._

  def submit(): (Long, Long) = {
    val list = param.inputs.map { input =>
      val sourceDF = getSourceDF(input)
      val inputDF = getInputDF(input)
      val (_idCnt, resDF) = filterDF(sourceDF, inputDF)
      (_idCnt, sourceDF, resDF)
    }

    val idCnt = list.map(_._1).sum
    val deviceCnt = persist(list.map(_._3).reduce(_ union _))
    list.foreach(_._2.unpersist())

    (idCnt, deviceCnt)
  }

  def persist(df: DataFrame): Long = {
    df.persist(StorageLevel.MEMORY_AND_DISK)
    val deviceCnt = df.count()
    val par = FnHelper.getCoalesceNum(deviceCnt)
    val df2 = df.coalesce(par)
    DeviceCacheWriter.insertTable2(spark, jobCommon, param.output,
      df2.toDF("data"),
      s"${JobName.getId(jobCommon.jobName)}|${DeviceType.DEVICE.id}")
    df.unpersist()

    deviceCnt
  }

  def getInputDF(input: CrowdFilterInput): Option[DataFrame] = {
    if (input.hasInput) {
      Some(DeviceSrcReader.toRDD2(spark, input).toDF("device").cache())
    } else {
      None
    }
  }

  def getSourceDF(input: CrowdFilterInput): DataFrame = {
    import spark._
    val includeFiltersBC = spark.sparkContext.broadcast(input.includeFilters)
    val excludeFiltersBC = spark.sparkContext.broadcast(input.excludeFilters)
    val numPattern = "[0-9|a-f]{40}".r
    sql(
      s"""
         |select
         | device,
         | coalesce(agebin,-1) as agebin,
         | coalesce(car,-1) as car,
         | cell_factory,
         | coalesce(edu,-1) as edu,
         | coalesce(gender,-1) as gender,
         | coalesce(income,-1) as income,
         | coalesce(kids,-1) as kids,
         | model,
         | coalesce(model_level,-1) as model_level,
         | coalesce(occupation,-1) as occupation,
         | tag_list,
         | catelist,
         | country,
         | province,
         | city,
         | permanent_country,
         | permanent_province,
         | permanent_city,
         | work_geohash,
         | home_geohash,
         | frequency_geohash_list,
         | applist
         |FROM ${PropUtils.HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE_VIEW}
         |WHERE device IS NOT NULL AND device <> '-1'
        """.stripMargin
    ).filter(r => {
      // todo device过滤逻辑抽取
      StringUtils.isNotBlank(r.getString(0)) && numPattern.pattern.matcher(r.getString(0)).find()
    }).filter{r =>
      val res1 = includeFiltersBC.value.forall{ case (c, f) => f.filter(r, c)}
      val res2 = excludeFiltersBC.value.exists{ case (c, f) => f.filter(r, c)}
      res1 && (!res2)
    }.select("device")
  }

  def filterDF(sourceDF: DataFrame, _inputDF: Option[DataFrame]): (Long, DataFrame) = {
    // 如果 input 没有定义直接 dmp 人群筛选
    var idCnt = 0L
    val resDF = if (_inputDF.nonEmpty) {
      val inputDF = _inputDF.get
      val joinFieldName = "device"
      idCnt = inputDF.count()
      println(s"broadcast filter $idCnt")
      if (idCnt != 0) {
        val col: Column = sourceDF("device")

        (if (idCnt > 10000000) { // 1kw
          inputDF.join(sourceDF, inputDF(joinFieldName) === sourceDF(joinFieldName))
        } else {
          broadcast(inputDF).join(sourceDF, inputDF(joinFieldName) === sourceDF(joinFieldName))
        }).drop(col)
      } else {
        sourceDF
      }
    }
    else {
      sourceDF
    }

    (idCnt, resDF)
  }
}
