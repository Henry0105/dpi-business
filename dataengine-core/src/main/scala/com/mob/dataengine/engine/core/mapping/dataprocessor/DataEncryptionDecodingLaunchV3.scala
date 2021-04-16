package com.mob.dataengine.engine.core.mapping.dataprocessor

import com.mob.dataengine.commons.DeviceSrcReader
import com.mob.dataengine.commons.enums.{EncryptType, InputType}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.{BaseJob2, DataEncryptionDecodingParamV3, JobContext2}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import com.mob.dataengine.commons.enums.SecDeviceType.PID


object DataEncryptionDecodingLaunchV3 extends BaseJob2[DataEncryptionDecodingParamV3] {
  val fieldName: String = "out_"
  val id: String = "id"
  val data: String = "data"
  val outputEncryptUDF = "out_encrypt"
  val inputEncryptUDF = "in_encrypt"
  val decodeAesUDF = "decode_aes"
  val decodeTransformUDF = "decode_transform"
  val PID_DECRYPT = "pid_decrypt"

  override def cacheInput: Boolean = false

  override def buildInputDataFrame(ctx: JobContext2[DataEncryptionDecodingParamV3]): DataFrame = {
    import ctx.param
    val srcDF = param.inputs.map(input =>
      DeviceSrcReader.toDFV2(ctx.spark, input, hubService = ctx.hubService).select(id, data))
      .reduce(_ union _)

    if (InputType.isDFS(param.inputTypeEnum) && param.sep.isDefined) {
      require(param.idx.nonEmpty)
      require(param.header > 0)
      require(param.headers.nonEmpty)
      require(param.headers.get.size >= param.idx.get)
      srcDF.filter(s"data != '${param.headers.get.map(_.substring(3)).mkString(param.sep.get)}'")
    } else {
      srcDF
    }
  }

  /**
   * 得到加密或者解密后的df
   * <p>
   * param.encryption = 0：加蜜；
   * param.encryption = _：解密
   * </p><pre>
   * 做解密操作：
   * 2 => AES 根据传入的args进行解密算法处理
   * _ => MD5_32(32位) MD5(16位) SHA256 其他 -> decode
   *
   * @param df 输入的df
   * @return 加解密之后的df
   */
  override def transformData(df: DataFrame, ctx: JobContext2[DataEncryptionDecodingParamV3]): DataFrame = {
    import ctx.param
    // 注册函数
    ctx.spark.udf.register(outputEncryptUDF, param.outputEncrypt.compute _)
    ctx.spark.udf.register(inputEncryptUDF, param.inputEncrypt.compute _)
    // aes解密方式的UDF
    ctx.spark.udf.register(decodeAesUDF, param.inputEncrypt.decode _)

    // pid解密成md5
    ctx.sql(s"create or replace temporary function $PID_DECRYPT as 'com.mob.udf.PidDecrypt'")
    // 解密时id字段transform的UDF
    ctx.spark.udf.register(decodeTransformUDF, param.transform.execute _)
    param.encryption match {
      case 0 =>  // 加密
        df.withColumn(s"$fieldName${param.inputIdTypeEnumSec}", callUDF(outputEncryptUDF, df(id)))
      case _ =>  // 解密
        // 分为2种, 一种是可以直接解密, 一种是需要撞库解密
        if (param.inputEncrypt.isAes && param.outputEncrypt.isNone) {  // 直接aes解密
          df.withColumn(s"$fieldName${param.inputIdTypeEnumSec}",
            callUDF(decodeAesUDF, df(id)))
        } else if (param.inputEncrypt.isPid && (param.outputEncrypt.isMd5 || param.outputEncrypt.isSha256)) {
          // pid解密成MD5/sha256
          df.withColumn(s"$fieldName${param.inputIdTypeEnumSec}",
            callUDF(PID_DECRYPT, df(id)))
        } else {
          decode(df, ctx)
        }
    }
  }

  /**
   * 非aes解密
   * <pre>
   * 先对mapping表的明文列做transfrom，之后inputDF leftjoin tableDF
   *
   * @param inputDF 需要解密的DF
   * @return 增加明文后的DF
   */
  def decode(inputDF: DataFrame, ctx: JobContext2[DataEncryptionDecodingParamV3]): DataFrame = {
    import ctx.param
    // 拿到最新分区
    val mappingTable = ctx.getSecSrcTable(param.inputIdTypeEnumSec)
    // 如果有transform操作，id字段就走transformUDF，否则忽略
    val tmpDF = if (param.transform.mapToObjSeq.isEmpty) {
      ctx.spark.table(mappingTable)
    } else {
      ctx.spark.table(mappingTable)
        .withColumn(s"${param.inputIdTypeEnumSec}", callUDF(decodeTransformUDF, col(s"${param.inputIdTypeEnumSec}")))
    }
    // 对id字段进行解密格式对应的加密操作
    val tableDF =
      tmpDF
        .withColumn("encryptCol", callUDF(inputEncryptUDF, col(s"${param.inputIdTypeEnumSec}")))
        .select(s"${param.inputIdTypeEnumSec}", "encryptCol")

    inputDF.cache()
    val idCnt = inputDF.count()
    // inputDF < 1kw 走broadcast join
    val df = if (idCnt > 10000000) inputDF else broadcast(inputDF)
    df.join(tableDF, inputDF(id) === tableDF(s"encryptCol"), "left")
      .drop(tableDF("encryptCol"))
      .withColumnRenamed(s"${param.inputIdTypeEnumSec}", s"$fieldName${param.inputIdTypeEnumSec}")
  }

  override def createMatchIdsCol(ctx: JobContext2[DataEncryptionDecodingParamV3]): Seq[Column] =
    Seq(lit(ctx.param.inputIdTypeEnumSec.id), concat_ws(",", col(s"$fieldName${ctx.param.inputIdTypeEnumSec}")))


  override def transformSeedDF(df: DataFrame, ctx: JobContext2[DataEncryptionDecodingParamV3]): DataFrame = {
    val dataFrame = if (ctx.param.output.keepSeed != 1) {
      df.select(s"${ctx.param.fieldNameSec}")
    } else {
      df.select("data", s"${ctx.param.fieldNameSec}")
    }
    dataFrame
  }

  override def persist2Hive(df: DataFrame, ctx: JobContext2[DataEncryptionDecodingParamV3]): Unit = {
    val cleanMapUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(clean_map _)
    // 做出match_ids列
    val outCols = createMatchIdsCol(ctx)
    df.withColumn("match_ids", cleanMapUDF(map(outCols: _*)))
      .createOrReplaceTempView("tmp")
    // 做出data列, 将输入数据中的其他列方式data中,并拼接
    ctx.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |partition(uuid='${ctx.param.output.uuid}', day='${ctx.jobCommon.day}')
         |select id, match_ids, ${ctx.param.inputIdTypeEnumSec.id} as id_type,
         | ${ctx.param.inputEncrypt.encryptType} as encrypt_type, data
         |from tmp
        """.stripMargin)

    persist2DataHub(df, ctx)
  }

  def persist2DataHub(df: DataFrame, ctx: JobContext2[DataEncryptionDecodingParamV3]): Unit = {
    import ctx.spark.implicits._
    import ctx.{hubService, param}
    val encryptTypes = getEncryptType(ctx)
    val id2Key = hubService.deduceIndex(param.inputIdTypeEnumSec.id.toString, encryptTypes._1)
    val out2Key = hubService.deduceIndex(param.inputIdTypeEnumSec.id.toString, encryptTypes._2)

    val resultDF = df.select(map(
      lit("seed"), split(col(data), param.sep.getOrElse("\u0001")),
      lit(id2Key), array($"id"),
      lit(out2Key), array(col(s"$fieldName${param.inputIdTypeEnumSec}"))).as("feature"
    ),
      col("data")
    )
    /** // todo 合并父数据
     * val seedDF = readFromSeedHub(ctx)
     * val mergeFeatureUDF = udf(mergeFeature(_: Map[String, Seq[String]], _: Map[String, Seq[String]]))
     * val featureDF = resultDF.join(seedDF, resultDF("data") === seedDF("seed"), "left")
     * .select(mergeFeatureUDF(resultDF("feature"), seedDF("feature")).as("feature"))
     */
    hubService.writeToHub(resultDF, param.output.uuid)
  }

  def getEncryptType(ctx: JobContext2[DataEncryptionDecodingParamV3]): (String, String) = {
    import ctx.param
    ctx.param.encryption match {
      case 0 => // 加密
        (EncryptType.NONENCRYPT.id.toString, param.outputEncrypt.encryptType.toString)
      case _ => // 解密
        (param.outputEncrypt.encryptType.toString, EncryptType.NONENCRYPT.id.toString)
    }
  }

  def mergeFeature(resultMap: Map[String, Seq[String]], seedMap: Map[String, Seq[String]]): Map[String, Seq[String]] = {
    val tmpRes =
      if (null == resultMap || resultMap.isEmpty) {
        seedMap
      } else if (null == seedMap || seedMap.isEmpty) {
        resultMap
      } else {
        resultMap ++ seedMap
      }
    tmpRes.filter { case (_, seq) => null != seq && seq.nonEmpty }
  }

  def readFromSeedHub(ctx: JobContext2[DataEncryptionDecodingParamV3]): DataFrame = {
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