package com.mob.dataengine.engine.core.mapping

import com.mob.dataengine.commons.enums.{DeviceType, JobName, MacArray}
import com.mob.dataengine.commons.pojo.{MatchInfo, OutCnt}
import com.mob.dataengine.commons.utils.FnHelper
import com.mob.dataengine.commons
import com.mob.dataengine.commons._
import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, IdMappingParam, JobContext}
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{broadcast, lower}

/**
 * @author juntao zhang
 */
object IdMapping extends BaseJob[IdMappingParam] {
  var idmappingJob: IdMappingJob = _

  override def run(): Unit = {
    val spark = jobContext.spark
    import spark.implicits._

    jobContext.params.foreach{ p =>
      println(p)
      val idRDD = p.inputs.map(input => DeviceSrcReader.toRDD2(jobContext.spark, input)).reduce(_ union _)
      idRDD.cache()
      val joinField = p.joinField
      val smallDeviceDF = idRDD.toDF(joinField)

      idmappingJob = IdMappingJob(jobContext.spark, smallDeviceDF, p, jobContext.jobCommon)

      idmappingJob.submit()

      if (jobContext.jobCommon.hasRPC()) {
        val matchInfoTmp = idmappingJob.matchInfoTmp
        val outCnt = new OutCnt
        val matchInfo = MatchInfo(jobContext.jobCommon.jobId, p.output.uuid, matchInfoTmp.idCnt,
          matchInfoTmp.matchCnt, outCnt)
        idmappingJob.idCounts.foreach { case (k, v) =>
          outCnt.set(k.toString + "_cnt", v)
        }

        val hasMacField = "mac".equals(joinField)
        if (hasMacField) {
          matchInfo.set("upload_ios_cnt", matchInfoTmp.iosUploadMac)
          matchInfo.set("upload_and_cnt", matchInfoTmp.idCnt - matchInfoTmp.iosUploadMac)
          matchInfo.set("match_ios_cnt", matchInfoTmp.iosMatchMac)
        }
        println(matchInfo.toJsonString())
        RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort,
          s"2\u0001${p.output.uuid}\u0002${matchInfo.toJsonString()}")
      }
    }
  }
}

case class IdMappingJob(
  spark: SparkSession,
  inputDF: DataFrame,
  p: IdMappingParam,
  jobCommon: JobCommon
) extends UDFCollections {
  import DeviceType._

  var resultDF: DataFrame = _
  var idCounts: Map[DeviceType, Long] = _
  var matchInfoTmp: MatchInfoTmp = MatchInfoTmp()
  val inputFieldEnum: DeviceType.DeviceType = p.idTypeEnum
  val encrypt: commons.Encrypt = p.inputs.head.encrypt
  val joinField: String = p.joinField
  val outputFields: String = p.getDeviceIdMappingHiveFields
  val outputIdTypesEnum: Seq[DeviceType] = p.output.idTypes.map(DeviceType(_))

  def submit(): Unit = {
    prepare()
    cal()
  }

  def prepare(): Unit = {
    spark.udf.register("aes", aes _)
    spark.udf.register("md5_array", md5Array _)
  }

  def cal(): Unit = {
    import spark.implicits._

    val _srcTable = IdMappingV2Job.getSrcTable(spark, inputFieldEnum)
    val isImei14Output = outputIdTypesEnum.contains(IMEI14)
    val srcTable = IdMappingV2Job.fakeImei14(spark, _srcTable, isImei14Output)
    val largeMappingDF = spark.sql(buildQuery(outputFields, srcTable))
    val encryptUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(encrypt.compute _)
    val joinCol = encryptUDF(largeMappingDF(joinField))

    resultDF = (if (inputDF.count() > 10000000) { // 1kw
      largeMappingDF.join(inputDF, lower(inputDF(joinField)) === lower(joinCol))
    } else {
      largeMappingDF.join(broadcast(inputDF), lower(inputDF(joinField)) === lower(joinCol))
    }).drop(largeMappingDF(joinField))

    resultDF.cache()

    matchInfoTmp.idCnt = inputDF.count()
    val hasMacField = "mac".equals(joinField)

    matchInfoTmp.matchCnt = resultDF.select(joinField).distinct().count()
    println(s"match cnt ${matchInfoTmp.matchCnt}")
    if (hasMacField) {
      matchInfoTmp.iosUploadMac = getMacarrayMatchCnt(spark, inputDF)
      matchInfoTmp.iosMatchMac = getMacarrayMatchCnt(spark, resultDF.select("mac"))
    }

    inputDF.unpersist()

    val m = outputIdTypesEnum.map { id =>
      (id, resultDF.select(s"$id").where(s"$id is not null")
        .flatMap(_.getAs[Seq[String]](s"$id")).filter(_.nonEmpty))
    }.toMap[DeviceType, Dataset[String]]
      .mapValues(outCol => outCol.map(p.output.encrypt.compute))

    idCounts = m.map { case (idType, outCol) =>
      val acc = spark.sparkContext.longAccumulator(s"acc_$idType")
      val par = FnHelper.getCoalesceNum(matchInfoTmp.matchCnt)
      val df = outCol.distinct().coalesce(par).map { r =>
        acc.add(1)
        r
      }.toDF("data")

      DeviceCacheWriter.insertTable2(spark, jobCommon, p.output, df,
        s"${JobName.getId(jobCommon.jobName)}|${idType.id}")
      (idType, acc.value.asInstanceOf[Long])
    }

    resultDF.unpersist()
  }

  def buildQuery(outputFields: String, table: String): String = {
    s"""
       |select $outputFields
       |from $table
     """.stripMargin
  }

  def getMacarrayMatchCnt(spark: SparkSession, df: DataFrame): Long = {
    import spark.implicits._
    val macArrayDF = spark.createDataset(MacArray.value).toDF("mac")
    val tmp = df.filter(r => r.getString(0).replaceAll(":", "").length == 6)
      .map(r => r.getString(0).replaceAll(":", "").toLowerCase().substring(0, 6)).toDF("mac")
    tmp.join(macArrayDF, tmp("mac") === macArrayDF("mac"))
      .count()
  }
}
