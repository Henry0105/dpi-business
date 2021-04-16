package com.mob.dataengine.engine.core.mapping

import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.enums.{DeviceType, MacArray}
import com.mob.dataengine.commons.pojo.{MatchInfo, OutCnt}
import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.commons.utils.{AesHelper, DateUtils, Md5Helper, PropUtils}
import com.mob.dataengine.commons.{DeviceSrcReader, Encrypt, JobCommon}
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, IdMappingV2Output, IdMappingV2Param, JobContext}
import com.mob.dataengine.rpc.RpcClient
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class MatchInfoTmp(
  var idCnt: Long = 0L,
  var matchCnt: Long = 0L,
  var iosUploadMac: Long = 0L,
  var iosMatchMac: Long = 0L)

object IdMappingV2 extends BaseJob[IdMappingV2Param] {
  var idmappingV2Job: IdMappingV2Job = _

  override def run(): Unit = {
    val spark = jobContext.spark
    import spark.implicits._

    // TODO 后期param多可以合并起来一起做
    jobContext.params.foreach(p => {
      println(p)

      val sep = p.sep
      val idRDD = p.inputs.map(input => DeviceSrcReader.toRDD2(jobContext.spark, input)).reduce(_ union _)
        .map(r => {
          if (sep.isDefined) {
            val idx = r.indexOf(sep.get)
            (r.substring(0, idx), r.substring(idx + 1))
          } else {
            (r, null)
          }
        })
      idRDD.cache()
      val smallDeviceDF = idRDD.toDF(p.joinField, "data")

      idmappingV2Job = IdMappingV2Job(jobContext.spark, smallDeviceDF, p, jobContext.jobCommon)
      idmappingV2Job.submit()

      idRDD.unpersist(blocking = false)
      val resultDF = idmappingV2Job.resultDF

      if (jobContext.jobCommon.hasRPC()) {
        idmappingV2Job.stat(jobContext.jobCommon.jobId)
        val matchInfo = idmappingV2Job.matchInfo
        RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort,
          s"2\u0001${p.output.uuid}\u0002${matchInfo.toJsonString()}")
      }

      resultDF.unpersist(blocking = false)
    })
  }
}

case class IdMappingV2Job(
  spark: SparkSession,
  inputDF: DataFrame,
  p: IdMappingV2Param,
  jobCommon: JobCommon
) extends UDFCollections {

  var matchInfo: MatchInfo = _
  var resultDF: DataFrame = _
  var idCounts: Map[DeviceType, Long] = _
  var matchInfoTmp: MatchInfoTmp = MatchInfoTmp()
  val inputFieldEnum: DeviceType.DeviceType = p.idTypeEnum
  val encrypt: Encrypt = p.inputs.head.encrypt
  val joinField: String = p.joinField
  val outputFields: String = p.getOutputFieldsWithKey.mkString(",")
  val outputIdTypesEnum: Seq[DeviceType] = p.output.idTypes.map(DeviceType(_))

  import DeviceType._

  private val isImei14Output = outputIdTypesEnum.contains(IMEI14)
  val encryptUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(encrypt.compute _)
  val idCnt: Long = inputDF.count()
  var matchCnt: Long = 0L

  def submit(): Unit = {
    prepare()
    cal()
    persist()
  }

  def prepare(): Unit = {
    spark.udf.register("aes", aes _)
    spark.udf.register("md5_array", md5Array _)
    spark.udf.register("latest", latest _)
    spark.udf.register("clean_map", clean_map _)
  }

  def cal(): Unit = {
    inputDF.createOrReplaceTempView("src_table")
    val _srcTable = IdMappingV2Job.getSrcTable(spark, inputFieldEnum)
    val srcTable = IdMappingV2Job.fakeImei14(spark, _srcTable, isImei14Output)

    var largeMappingDF = spark.sql(
      s"""
         |select $outputFields
         |from $srcTable
         """.stripMargin)
    largeMappingDF = largeMappingDF.withColumn(joinField, encryptUDF(largeMappingDF(p.origJoinField)))

    val col: Column = largeMappingDF(joinField)

    val tmp = if (inputDF.count() > 10000000) { // 1kw
      inputDF.join(largeMappingDF, lower(inputDF(joinField)) === lower(largeMappingDF(joinField)), "left")
    } else {
      broadcast(inputDF)
        .join(largeMappingDF, lower(inputDF(joinField)) === lower(largeMappingDF(joinField)), "left")
    }

    tmp.cache()
    matchCnt = tmp.filter(col.isNotNull).count()

    resultDF = tmp.drop(col)
    resultDF.createOrReplaceTempView("result_table")
  }

  def persist(): Unit = {
    spark.udf.register("aes", aes _)

    persist2Hive(p.output, joinField)
    export2Hdfs(p.output, joinField)

    spark.sql("drop temporary function aes")
  }

  def getOutputsFields: Seq[String] = {
    p.output.idTypes.map(DeviceType(_)).toSet[DeviceType].map(_.toString).toSeq
  }

  def stat(jobId: String): Unit = {
    val matchInfoTmp = MatchInfoTmp(idCnt = idCnt, matchCnt = matchCnt)
    val hasMacField = "mac".equals(joinField)
    println(s"match cnt ${matchInfoTmp.matchCnt}")

    if (hasMacField) {
      matchInfoTmp.iosUploadMac = getMacarrayMatchCnt(spark, inputDF)
      matchInfoTmp.iosMatchMac = getMacarrayMatchCnt(spark, resultDF.select("mac"))
    }

    // 统计各个指标
    val fieldsStatMap = getOutputsFields.filterNot(_.equals(joinField))
      .map(f => f -> statCol(spark, f, "result_table")).toMap

    val outCnt = new OutCnt
    p.output.idTypesEnum.filterNot(_.equals(inputFieldEnum))
      .foreach(idEnum => outCnt.set(s"${idEnum.toString}_cnt", fieldsStatMap(idEnum.toString)))

    matchInfo = MatchInfo(jobId, p.output.uuid, matchInfoTmp.idCnt, matchInfoTmp.matchCnt, outCnt)
    if (hasMacField) {
      matchInfo.set("upload_ios_cnt", matchInfoTmp.iosUploadMac)
      matchInfo.set("upload_and_cnt", matchInfoTmp.idCnt - matchInfoTmp.iosUploadMac)
      matchInfo.set("match_ios_cnt", matchInfoTmp.iosMatchMac)
    }
    println(matchInfo.toJsonString())
  }

  def getMacarrayMatchCnt(spark: SparkSession, df: DataFrame): Long = {
    import spark.implicits._
    val macArrayDF = spark.createDataset(MacArray.value).toDF("mac")
    val tmp = df.filter(r => r.getString(0).replaceAll(":", "").length == 6)
      .map(r => r.getString(0).replaceAll(":", "").toLowerCase().substring(0, 6)).toDF("mac")
    tmp.join(macArrayDF, tmp("mac") === macArrayDF("mac"))
      .count()
  }

  def statCol(spark: SparkSession, field: String, table: String): Long = {
    import spark.implicits._
    spark.sql(
      s"""
         |select $field
         |from $table
         |where $field is not null
       """.stripMargin)
      .flatMap(_.getAs[String](field).split(",")).distinct().count()
  }

  def getOutputFields(output: IdMappingV2Output, inputType: DeviceType): Seq[String] = {
    val suffix = output.encrypt.suffix
    val stringFieldFormater = output.encrypt.encryptType match {
      case 0 => "%s as %s"
      case 1 => "md5(%s) as %s"
      case 2 => s"aes(%s, '${output.encrypt.args.get("key")}', '${output.encrypt.args.get("iv")}') as %s"
    }

    output.idTypesEnum.map{ idType =>
      stringFieldFormater.format(s"$idType", s"$idType$suffix")
    }
  }

  def persist2Hive(out: IdMappingV2Output, joinField: String): Unit = {
    val outputSuffix = out.encrypt.suffix
    val outputField2MapField = out.idTypesEnum.map{idType =>
      s"${idType.id},$idType$outputSuffix"
    }.mkString(",")

    spark.sql(
      s"""
         |select $joinField,
         |  ${if (outputField2MapField.isEmpty) null else s"map($outputField2MapField)"} match_ids, data
         |from (
         |  select ${(getOutputFields(out, inputFieldEnum) :+ joinField).mkString(",")}, data
         |  from result_table
         |) as tmp
           """.stripMargin).createOrReplaceTempView("final_table")

    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |partition(uuid='${out.uuid}', day='${DateUtils.currentDay()}')
         |select $joinField, clean_map(match_ids) match_ids, ${inputFieldEnum.id} as id_type,
         |  ${out.encrypt.encryptType} as encrypt_type, data
         |from final_table
           """.stripMargin)
  }

  def export2Hdfs(out: IdMappingV2Output, joinField: String): Unit = {
    // 写入hdfs
    val map2Fields = out.idTypesEnum.map{idType =>
      val suffix = out.encrypt.suffix
      s"match_ids[${idType.id}] as $idType$suffix"
    }
    // 如果有sep字段则需要将数据复原
    val idField = if (p.sep.isDefined) {
      s"concat_ws('${p.sep.get}', $joinField, data) as data"
    } else {
      joinField
    }
    val df = spark.sql(
      s"""
         |select ${(idField :: map2Fields.toList).mkString(",")}
         |from final_table
         |${if (out.limit.nonEmpty) s"limit ${out.limit.get}" else ""}
           """.stripMargin)

    df.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header", "true").option("sep", "\t")
      .option("emptyValue", null)
      .csv(out.hdfsOutput)
  }
}

object IdMappingV2Job extends UDFCollections {
  import DeviceType._

  def getLastPar(spark: SparkSession, table: String): String = {
    spark.sql(s"show partitions $table").collect().map(_.getAs[String](0).split("/")(0)).max
  }

  def getSrcTable(spark: SparkSession, idType: DeviceType): String = {
    idType match {
      case PHONE => PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3
      case MAC => PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3
      case DEVICE => PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3
      case IMEI => PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3
      case IMEI14 => PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3  // 都走imei的表,不走imei14的表
    }
  }

  def fakeImei14(spark: SparkSession, src: String, isImei14Output: Boolean): String = {
    val par = getLastPar(spark, src)
    val fakeTable = "fake_table"
    val trim14UDF = org.apache.spark.sql.functions.udf(trim2Imei14 _)
    (if (src.contains("imei") && isImei14Output) { // imei为主键并且输出imei14
      spark.table(src).filter(par).withColumn(IMEI14.toString, array(substring(col("imei"), 0, 14)))
        .withColumn(s"${IMEI14.toString}_tm", array(col("update_time")))
    } else if (src.contains("imei")) {
      spark.table(src).filter(par).withColumn(IMEI14.toString, substring(col("imei"), 0, 14))
    } else {  // imei为array类型的需要用udf转换
      spark.table(src).filter(par).withColumn(IMEI14.toString, trim14UDF(col("imei")))
        .withColumn(s"${IMEI14.toString}_tm", col("imei_tm"))
    } ).createOrReplaceTempView(fakeTable)
    fakeTable
  }
}

