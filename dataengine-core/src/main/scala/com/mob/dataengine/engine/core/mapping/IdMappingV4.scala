package com.mob.dataengine.engine.core.mapping

import java.util.regex.Pattern

import com.mob.dataengine.commons.enums.{InputType, McidArray, SecDeviceType}
import com.mob.dataengine.commons.pojo.{MatchInfoV2, OutCnt}
import com.mob.dataengine.commons.utils.{PropUtils, Sha256Helper}
import com.mob.dataengine.commons.{DeviceSrcReader, Encrypt}
import com.mob.dataengine.engine.core.jobsparam.{BaseJob2, IdMappingV4Param, JobContext2}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec

case class MatchInfoTmpV2(
  var idCnt: Long = 0L,
  var matchCnt: Long = 0L,
  var iosUploadMcid: Long = 0L,
  var iosMatchMcid: Long = 0L)

object IdMappingV4 extends BaseJob2[IdMappingV4Param] {

  override def cacheInput: Boolean = true

  /**
   * @param ctx 任务上下文
   * */
  override def createMatchIdsCol(ctx: JobContext2[IdMappingV4Param]): Seq[Column] = null

  val outputColPrefix = "out_"
  val renamedSuffix = "_renamed"
  val uniqueID = "tmp_uuid" // 用来表示每行数据唯一的id
  val isBkKey = "isBk"
  val PID_ENCRYPT = "pid_encrypt"
  val AES_ENCRYPT = "aes_encrypt"
  val PID_DECRYPT = "pid_decrypt"
  val UNIFY_MCID = "unify_mcid"
  val SHA256 = "sha256"
  val identityUDF: Column => Column = identity[Column]

  import SecDeviceType._

  override def run(ctx: JobContext2[IdMappingV4Param]): Unit = {
    println(ctx)

    // 注册加密函数
//    ctx.sql(s"drop function IF EXISTS $PID_ENCRYPT")
//    ctx.sql(s"drop function IF EXISTS $AES_ENCRYPT")
//    ctx.sql(s"drop function IF EXISTS $PID_DECRYPT")
    ctx.sql(s"create or replace temporary function $PID_ENCRYPT as 'com.mob.udf.PidEncrypt'")
    ctx.sql(s"create or replace temporary function $AES_ENCRYPT as 'com.mob.udf.AesEncrypt'")
    ctx.sql(s"create or replace temporary function $PID_DECRYPT as 'com.mob.udf.PidDecrypt'")
    ctx.spark.udf.register(UNIFY_MCID, unifyMcid _)
    ctx.spark.udf.register(SHA256, Sha256Helper.entrySHA256 _)

    val param = ctx.param
    val isBk = param.deviceMatch == 2 && PID.equals(param.inputIdTypeEnumSec) &&
      param.output.idTypesEnumSec.contains(DEVICE)
    ctx.putStatus(isBkKey, isBk)
    val buildDF = buildInputDataFrame(ctx)
    if (cacheInput) buildDF.cache()
    ctx.matchInfo.idCnt = buildDF.count()
    // 对输入数据的字段处理
    // 增加一个唯一id,为了确保 匹配device且deviceMatch = 1时，数据丢失问题
    val inputDF = addJoinField2DataFrame(buildDF, true, ctx)
      .withColumn(uniqueID, monotonically_increasing_id())
    var transDF = transformData(inputDF, ctx)
    transDF.persist(StorageLevel.MEMORY_AND_DISK)
    ctx.matchInfo.setMatchCnt(transDF.count())
    val tmpDF = if (param.output.idTypesEnumSec.contains(DEVICE) && param.deviceMatch.equals(1)
      && param.inputIdTypeEnumSec != SecDeviceType.DEVICE) {
      filterDeviceByTags(transDF, ctx)
    } else if (isBk) {
      backTrackMatch(transDF)
    } else {
      transDF
    }
    val unionDF = unionUnJoined(tmpDF.drop(uniqueID), inputDF, param).persist(StorageLevel.DISK_ONLY)
    stat(unionDF, ctx)

    transDF = transform2OptCache(unionDF.repartition(getCoalesceNum(ctx.matchInfo.matchCnt, 100000)), ctx)
    persist2Hive(transDF, ctx)
    if (StringUtils.isNotBlank(ctx.param.output.hdfsOutput)) {
      write2HDFS(transDF, ctx)
    }
    sendRPC(ctx)
  }

  def unionUnJoined(joinedDF: DataFrame, inputDF: DataFrame, param: IdMappingV4Param): DataFrame = {
    val nullFieldsList = joinedDF.schema.fieldNames
      .filter(name => name == param.rightField || name.startsWith("out")) diff inputDF.schema.fieldNames
    var notJoinedDF = broadcast(inputDF)
      .join(joinedDF.drop(inputDF.schema.fieldNames: _*),
        lower(inputDF(param.leftField)) === lower(joinedDF(param.rightField)), "left")
      .filter(joinedDF(param.rightField).isNull).drop(uniqueID)
    for (colName <- nullFieldsList) {
      if (param.output.idTypesEnumSec.contains(DEVICE) && param.deviceMatch.equals(1)
        && param.inputIdTypeEnumSec != SecDeviceType.DEVICE) {
        if (Set(s"${SecDeviceType.DEVICE}_ltm", s"out_${SecDeviceType.DEVICE}").contains(colName)) {
          notJoinedDF = notJoinedDF.drop(colName).withColumn(colName, array(lit("")))
        } else {}
      }
    }
    joinedDF.union(notJoinedDF)
  }

  override def transformData(inputDF: DataFrame, ctx: JobContext2[IdMappingV4Param]): DataFrame = {
    val param = ctx.param
    // 根据inputIdTypeEnumSec选取合适的mapping表，并且进行字段处理
    val origMappingDF = addJoinField2DataFrame(buildMappingTable(
      ctx.spark, param.inputIdTypeEnumSec, param), false, ctx)
    var _transformDF = origMappingDF
    if (param.output.matchOrigIeid.get == 1 && param.output.idTypesEnumSec.toSet.contains(IEID)) {
      _transformDF = origMappingDF
        .drop(s"ieid", s"ieid_tm", s"ieid_ltm", "ieid_md5")
        .withColumnRenamed(s"orig_ieid", s"ieid")
        .withColumnRenamed(s"orig_ieid_tm", s"ieid_tm")
        .withColumnRenamed(s"orig_ieid_ltm", s"ieid_ltm")
    }
    val transformedDF = transformMappingTable(param, _transformDF)

    inputDF.cache()
    val idCnt = inputDF.count()
    val joinCondition = lower(inputDF(param.leftField)) === lower(transformedDF(param.rightField))

    // 1kw以下走广播join
    if (idCnt > 10000000) { // 1kw
      inputDF.join(transformedDF, joinCondition, "inner")
    } else {
      broadcast(inputDF).join(transformedDF, joinCondition, "inner")
    }
  }

  def initTagsDF(ctx: JobContext2[IdMappingV4Param]): DataFrame = {
    ctx.sql(
      s"""
         |SELECT device, size(tags) tags_size
         |FROM ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_VIEW}
         |WHERE tags is not null
       """.stripMargin)
      .toDF("device", "tags_size")
  }

  // 取标签最多的device
  def filterDeviceByTags(df: DataFrame, ctx: JobContext2[IdMappingV4Param]): DataFrame = {
    var tmp = df
    val tagsDF = initTagsDF(ctx)
    val deviceColName = s"$outputColPrefix$DEVICE"
    val deviceDF = df.select(explode(col(deviceColName))).distinct().toDF(deviceColName)
    val deviceTagsSizeDF = deviceDF.join(tagsDF, deviceDF(deviceColName) === tagsDF("device"), "inner")
      .drop(tagsDF("device"))
      .toDF("device", "tags_size")
      .cache()

    ctx.spark.udf.register("zip_cols", (c1: Seq[String], c2: Seq[String]) => {
      if (null == c1 || null == c2) { // 因为是left join得到的device,所以会出现为null的情况
        Seq(("", ""))
      } else {
        c1.zip(c2)
      }
    })
    tmp = tmp.withColumn("zip_cols",
      explode(callUDF("zip_cols", col(deviceColName), col(s"${DEVICE}_ltm"))))
    tmp.join(deviceTagsSizeDF, tmp("zip_cols._1") === deviceTagsSizeDF("device"), "left")
      .withColumn("rk",
        row_number().over(Window.partitionBy(ctx.param.leftField, uniqueID).orderBy(col("tags_size").desc)))
      .filter("rk = 1")
      .withColumn(deviceColName, array(col("zip_cols._1")))
      .withColumn(s"${DEVICE}_ltm", array(col("zip_cols._2")))
      .drop("zip_cols", "tags_size", "rk")
      .drop(deviceTagsSizeDF("device"))
  }

  private def binarySearch[T](seq: Seq[T], elem: T)(implicit ord: Ordering[T]): Int = {
    @tailrec
    def subSearch(from: Int, to: Int)(implicit ord: Ordering[T]): Int = {
      if (to == from) from - 1 else {
        val idx = from + (to - from - 1) / 2
        math.signum(ord.compare(elem, seq(idx))) match {
          case -1 => subSearch(from, idx)(ord)
          case 1 => subSearch(idx + 1, to)(ord)
          case _ => idx
        }
      }
    }

    subSearch(0, seq.length - 1)(ord)
  }

  def backTrackIndex(bkTm: Seq[String], trackTm: String): Int = {
    val bkTmL = bkTm.map(_.toLong)
    val trackTmL = trackTm.toLong
    val index = if (trackTmL >= bkTmL.last) bkTmL.length - 1 else {
      binarySearch[Long](bkTmL, trackTmL)
    }
    if (index < 0) 0 else index
  }

  // phone => device 回溯匹配
  def backTrackMatch(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    val delta = 1 // sql数组以1开始,需要补位
    val bk_index = udf(backTrackIndex(_: Seq[String], _: String))
    val tmpCol = "bk_tm_index"
    val tmpCol2 = "device_index_index"
    df.withColumn(s"$tmpCol",
      bk_index($"bk_tm", unix_timestamp(col("day"), "yyyyMMdd")) + lit(delta))
      .withColumn(s"$tmpCol2", element_at($"device_index", col(tmpCol)) + lit(delta))
      .withColumn(s"out_device", array(element_at($"out_device", col(tmpCol2))))
      .withColumn(s"update_time", element_at($"bk_tm", col(tmpCol)))
      .drop("index", tmpCol, tmpCol2)
  }

  def stat(df: DataFrame, ctx: JobContext2[IdMappingV4Param]): Unit = {
    val param = ctx.param
    val matchInfoTmp = MatchInfoTmpV2(idCnt = ctx.matchInfo.idCnt, matchCnt = ctx.matchInfo.matchCnt)
    val hasMcidField = MCID.equals(param.inputIdTypeEnumSec)
    println(s"match cnt ${matchInfoTmp.matchCnt}")

    if (hasMcidField) {
      matchInfoTmp.iosUploadMcid = getMcidarrayMatchCnt(ctx.spark, df.select(param.leftField))
      matchInfoTmp.iosMatchMcid = getMcidarrayMatchCnt(ctx.spark,
        df.select(param.rightField).filter(col(param.rightField).isNotNull))
    }

    // 统计各个指标
    val fieldsStatMap = param.output.idTypesEnumSec.diff(Seq(param.inputIdTypeEnumSec)).map { f =>
      val outField = s"$outputColPrefix$f"
      (s"$f", statCol(ctx.spark, outField, df))
    }.toMap

    val outCnt = new OutCnt
    param.output.idTypesEnumSec.filterNot(_.equals(param.inputIdTypeEnumSec))
      .foreach(idEnum => outCnt.set(s"${idEnum}_cnt", fieldsStatMap(s"$idEnum")))

    ctx.matchInfo = MatchInfoV2(matchInfoTmp.idCnt, matchInfoTmp.matchCnt, outCnt, param.output.uuid)
    if (hasMcidField) {
      ctx.matchInfo.set("upload_ios_cnt", matchInfoTmp.iosUploadMcid)
      ctx.matchInfo.set("upload_and_cnt", matchInfoTmp.idCnt - matchInfoTmp.iosUploadMcid)
      ctx.matchInfo.set("match_ios_cnt", matchInfoTmp.iosMatchMcid)
    }
    println(ctx.matchInfo.toJsonString())
  }

  def getMcidarrayMatchCnt(spark: SparkSession, df: DataFrame): Long = {
    import spark.implicits._
    val mcidArrayDF = spark.createDataset(McidArray.value).toDF("mcid")
    val tmp = df.filter(r => r.getString(0).replaceAll(":", "").length == 6)
      .map(r => r.getString(0).replaceAll(":", "").toLowerCase().substring(0, 6)).toDF("mcid")
    tmp.join(mcidArrayDF, tmp("mcid") === mcidArrayDF("mcid"))
      .count()
  }

  def statCol(spark: SparkSession, field: String, df: DataFrame): Long = {
    import spark.implicits._
    df.select(field).filter(col(field).isNotNull)
      .flatMap(_.getAs[Seq[String]](field)).distinct().count()
  }

  def transform2OptCache(df: DataFrame, ctx: JobContext2[IdMappingV4Param]): DataFrame = {
    val param = ctx.param
    val cleanMapUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(clean_map _)
    var tmp = df
    // 做出match_ids列
    val outCols: Seq[Column] = param.output.idTypesEnumSec.flatMap { c =>
      Seq(lit(c.id), concat_ws(",", col(s"$outputColPrefix$c")))
    }
    val outLtmCols = param.output.idTypesEnumSec.flatMap { c =>
      if (ctx.getStatus[Boolean](isBkKey) || c.equals(param.inputIdTypeEnumSec)) {
        Seq(lit(c.id + 100), col("update_time"))
      } else {
        Seq(lit(c.id + 100), concat_ws(",", col(s"${c}_ltm")))
      }
    }
    val newOutCols = outCols ++ outLtmCols
    tmp = tmp.withColumn("match_ids", cleanMapUDF(map(newOutCols: _*)))
    // .withColumn("match_ltms", cleanMapUDF(map(outLtmCols: _*)))

    // 做出id列
    tmp.withColumn("id", col(param.origJoinField))
  }

  override def persist2Hive(df: DataFrame, ctx: JobContext2[IdMappingV4Param]): Unit = {
    val param = ctx.param
    df.createOrReplaceTempView("tmp")

    ctx.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |partition(uuid='${param.output.uuid}', day='${ctx.jobCommon.day}')
         |select id, match_ids, ${param.inputIdTypeEnumSec.id} as id_type,
         |  ${param.inputEncrypt.encryptType} as encrypt_type, data
         |from tmp
        """.stripMargin)

    // todo 这里以后只写一个表 rp_data_hub
    persist2DataHub(ctx)
  }

  def persist2DataHub(ctx: JobContext2[IdMappingV4Param]): Unit = {
    val id2Key = ctx.param.output.idTypes.map{ id =>
      val key = ctx.hubService.deduceIndex(id.toString, ctx.param.output.encrypt.encryptType.toString)
      (id, key)
    }.toMap

    val otherFields = ctx.param.output.idTypes.map { id =>
      // code, id, ltm
      s"""
         |'${id2Key(id)}',
         |if(match_ids[$id] is not null, array(match_ids[$id], match_ids[${id + 100}]), null)
        """.stripMargin
    }.mkString(",")

    // todo 这里输入为sql的时候有问题
    ctx.param.inputs.map{ input =>
      ctx.sql(
        s"""
           |select feature
           |from ${PropUtils.HIVE_TABLE_DATA_HUB}
           |where uuid = '${input.uuid}'
          """.stripMargin)
    }.reduce(_ union _).createOrReplaceTempView("seed_tb")

    ctx.spark.udf.register("merge_feature", (resultMap: Map[String, Seq[String]],
    seedMap: Map[String, Seq[String]]) => {
      val tmpRes =
        if (null == resultMap || resultMap.isEmpty) {
          seedMap
        } else if (null == seedMap || seedMap.isEmpty) {
          resultMap
        } else {
          resultMap ++ seedMap
        }
      tmpRes.filter{ case (_, seq) => null != seq && seq.nonEmpty}
    })

    // todo 这里会有多维过滤加idmapping的时候传很多画像数据过来的情况,暂时不取feature的数据
    ctx.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_HUB} partition(uuid='${ctx.param.output.uuid}')
         |select map('seed', array(data), $otherFields)
         |from tmp
        """.stripMargin)
  }

  override def transformSeedDF(df: DataFrame, ctx: JobContext2[IdMappingV4Param]): DataFrame = {
    val dataFrame = if (ctx.param.output.keepSeed != 1) {
      df.select(s"${ctx.param.fieldName}")
    } else {
      df.select("data", s"${ctx.param.fieldName}")
    }
    dataFrame
  }

  override def write2HDFS(df: DataFrame, ctx: JobContext2[IdMappingV4Param]): Unit = {
    df.createOrReplaceTempView("final_table")
    val param = ctx.param
    // 作出匹配的数据列和时间列
    val map2Fields = param.output.idTypesEnumSec.map { idType =>
      val suffix = param.output.encrypt.suffix
      val c0 = s"match_ids[${idType.id}]"
      val c1 = s"$idType$suffix"
      val c2 = c0 + " as " + c1
      val c3 = s"${idType.id}"
      //      val c2 = s"match_ltms[${idType.id}] as $idType${suffix}_ltm"
      (c1, c2, c3)
    }

    // 如果有sep字段则需要将数据复原
    val idField = if (param.sep.isDefined) {
      s"concat_ws('${param.sep.get}', ${param.headers.get.mkString(",")}) as `${param.headers.get.mkString(",")}`"
    } else {
      param.origJoinField
    }

    // keepSeed: 0 表示导出的时候(写入hive的数据不变, hdfs即导出包不带种子数据)不保留种子数据, 并且将数据都放在一列中
    // 不同的idType写入不同的目录下
    if (param.output.keepSeed == 0) {
      val tmpStr = map2Fields.map { case (idTypeSuffix, matchIdsAsIdTypeSuffix, idType) =>
        s"""
           |( select
           |    first(idType) as idType, match_id
           |from
           | (  select '$idType' as idType, $matchIdsAsIdTypeSuffix
           |    from final_table
           | ) as res_tmp
           | lateral view explode(split($idTypeSuffix, ',')) table_tmp as match_id
           | group by match_id
           | ${if (param.output.limit.get > 0) s"limit ${param.output.limit.get}" else ""})
         """.stripMargin
      }.mkString(" union all ")

      ctx.spark.sql(tmpStr)
        .write.mode(SaveMode.Overwrite)
        .option("header", "false").option("sep", "\t")
        .option("emptyValue", null)
        .partitionBy("idType")
        .csv(param.output.hdfsOutput)
    } else {
      val resDF = ctx.spark.sql(
        s"""
           |select ${(idField :: map2Fields.map(_._2).toList).mkString(",")}
           |from final_table
           |${if (param.output.limit.get > 0) s"limit ${param.output.limit.get}" else ""}
        """.stripMargin)

      resDF.repartition(1).write.mode(SaveMode.Overwrite)
        .option("header", "true").option("sep", "\t")
        .option("emptyValue", null)
        .csv(param.output.hdfsOutput)
    }
  }

  /**
   * 给dataframe添加需要进行join的key 大小写/mcid清理/加密处理
   *
   * @param srcDF   要处理的df
   * @param isInput 表示数据是不是input,因为input的列已经做过某种处理(例如: 加密)
   * @param ctx  任务参数
   * @return 处理过后的df
   */
  def addJoinField2DataFrame(srcDF: DataFrame, isInput: Boolean, ctx: JobContext2[IdMappingV4Param]): DataFrame = {
    val unifyJoinFieldUDF = unifyJoinField(isInput, ctx)
    val origJoinField = if (isInput) {
      ctx.param.origJoinField
    } else {
      s"${ctx.param.inputIdTypeEnumSec}"
    }
    val joinField = if (isInput) {
      ctx.param.leftField
    } else {
      ctx.param.rightField
    }
    srcDF.withColumn(joinField, unifyJoinFieldUDF(col(origJoinField)))
  }

  // 这里重新做一个新列出来用来join数据
  // 对种子数据做一个列/对mapping表做一个列, 然后后面使用这2个列进行join
  /**
   * @param isInput 标识数据是不是种子数据
   */
  def unifyJoinField(isInput: Boolean, ctx: JobContext2[IdMappingV4Param]): Column => Column = {
    if (isInput) {
      if (ctx.param.inputEncrypt.isNone) { // 如果是明文,需要对输入数据进行加密
        ctx.param.inputIdTypeEnumSec match {
          case PID => callUDF(PID_ENCRYPT, _)
          case DEVICE => identityUDF
          case MCID =>
            val f1: Column => Column = callUDF(UNIFY_MCID, _)
            f1 andThen lower andThen md5
          case IFID => upper _ andThen md5
          case _ => lower _ andThen md5
        }
      } else if (ctx.param.inputEncrypt.isMd5) {
        ctx.param.inputIdTypeEnumSec match {
          case PID => val f: Column => Column = callUDF(AES_ENCRYPT, _)
            lower _ andThen f
          case _ => lower _
        }
      } else if (ctx.param.inputEncrypt.isSha256) { // sha256只对pid处理, 不和上面的情况合并
        ctx.param.inputIdTypeEnumSec match {
          case PID => val f: Column => Column = callUDF(AES_ENCRYPT, _)
            lower _ andThen f
          case _ => lower _
        }
      } else { // 如果是其他加密方式,则不支持,输入是sha256则在mapping表那边处理
        lower _
      }
    } else { // 如果是mapping表,则不做处理,有需要的话,在mapping表做
      identityUDF
    }
  }

  // 对mcid添加冒号
  def unifyMcid(s: String): String = {
    if (s.contains(":")) {
      s
    } else {
      s.replaceAll(".{2}(?=.)", "$0:")
    }
  }

  override def buildInputDataFrame(ctx: JobContext2[IdMappingV4Param]): DataFrame = {
    val param = ctx.param
    val srcDF = param.inputs.map(input =>
      DeviceSrcReader.toDFV2(ctx.spark, input, param.trackDay, param.trackDayIndex,
        ctx.getStatus[Boolean](isBkKey), ctx.hubService)).reduce(_ union _)
    if (InputType.isSql(param.inputTypeEnum)) {
      param.headers = Option(srcDF.schema.fieldNames.toSeq.filter(_.startsWith("in_")))
    }

    if (param.sep.isDefined) {
      // 这里header一定是有的,python端会做处理将源文件中的header去掉,参数里面传header进来
      // 并带上 `in_` 这样的前缀
      require(param.idx.nonEmpty)
      require(param.header > 0)
      require(param.headers.nonEmpty)
      require(param.headers.get.size >= param.idx.get)

      // 去掉headers行,因为加了前缀 in_,所以要做substring
      var tmpDF = if (!InputType.isSql(param.inputTypeEnum)) {
        srcDF.filter(s"data != '${param.headers.get.map(_.substring(3)).mkString(param.sep.get)}'")
      } else {
        srcDF
      }

      // 将data字段拆分开处理
      param.headers.get.zipWithIndex.foreach{ case (f, idx) =>
        tmpDF = tmpDF.withColumn(f, split(col("data"), Pattern.quote(param.sep.get)).getItem(idx))
      }
      // todo 这步也要修正一下
      if (tmpDF.schema.fieldNames.contains(param.origJoinField)) {
        tmpDF = tmpDF.drop(param.origJoinField)
      }

      tmpDF = tmpDF.withColumnRenamed("id", param.origJoinField)

      tmpDF
    } else {
      srcDF.withColumnRenamed("id", param.origJoinField)
    }
  }

  /**
   * 根据mapping表做出需要导出的列, 要导出的列都是array类型
   * 1. 匹配个数
   * 2. 加密
   * 3. 添加输出的列,名称加前缀out_,避免与原有df的列名造成冲突
   */
  def transformMappingTable(param: IdMappingV4Param, df: DataFrame): DataFrame = {
    val latestArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(latestArray _)
    val latestArrayLtmUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(latestLtmArray _)
    val outputEncrypt: Encrypt = Encrypt(0)
    // 不做加密
    val outputEncryptUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(Encrypt(0).compute _)
    val outputEncryptArray: (Seq[String], String) => Seq[String] =
      (seq: Seq[String], deviceType: String) => {
        if (null == seq || seq.isEmpty) {
          null
        } else if (deviceType != DEVICE.toString && !deviceType.contains("md5")) {
          seq.map(outputEncrypt.compute)
        } else {
          seq
        }
      }
    val outputEncryptArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(outputEncryptArray)

    val outputColsEnum = param.output.idTypesEnumSec.diff(Seq(param.inputIdTypeEnumSec))
    var tmpDF = df
    // 对要匹配输出的列进行处理
    outputColsEnum.foreach { colEnum =>
      val outCol = s"$outputColPrefix$colEnum"
      if (DEVICE.equals(colEnum) && param.deviceMatch != 0) { // 输出有device 且格式为非默认方式
        tmpDF = tmpDF.withColumn(outCol, col("device"))
      } else {
        val transCol = if (param.output.encrypt.encryptType == 1 &&
          tmpDF.schema.fieldNames.contains(s"${colEnum}_md5")) {
          s"${colEnum}_md5"
        } else s"$colEnum"
        val transformedCol = outputEncryptArrayUDF(latestArrayUDF(
          when(tmpDF(transCol).isNull && transCol.contains("md5"),
            col(transCol.replaceAll("_md5", ""))).otherwise(col(transCol)),
          col(s"${colEnum}_ltm"), lit(param.output.matchLimit.getOrElse(1))),
          when(tmpDF(transCol).isNull && transCol.contains("md5"),
            lit(transCol.replaceAll("_md5", ""))).otherwise(lit(transCol)))
        val transformedLtmCol = latestArrayLtmUDF(col(s"${colEnum}_ltm"),
          lit(param.output.matchLimit.getOrElse(1)))
        tmpDF = tmpDF.withColumn(outCol, transformedCol)
          .withColumn(s"${colEnum}_ltm", transformedLtmCol)
      }
    }

    // 如果要输出列中包含输入类型,即: 输入mcid,输出mcid的MD5等
    if (param.output.idTypesEnumSec.contains(param.inputIdTypeEnumSec)) {
      val transCol = if (param.output.encrypt.encryptType != 1 || param.inputIdTypeEnumSec == DEVICE) {
        outputEncryptUDF(col(s"${param.inputIdTypeEnumSec}"))
      } else {
        col(s"${param.inputIdTypeEnumSec}_md5")
      }
      tmpDF = tmpDF.withColumn(s"$outputColPrefix${param.inputIdTypeEnumSec}", transCol)
    }

    tmpDF
  }

  /**
   * 给mapping表加imei14/imei15两列,分2中情况
   * 1. mapping表是imei_mapping
   * 2. 其他mapping表
   */
//  def withIeidColumns(df: DataFrame, param: IdMappingV4Param): DataFrame = {
//    val cleanedDF = if (1 == param.cleanIeid) {
//      if (param.isIeidTable) {
//        df.filter("ieid rlike '^[0-9]+$' ")
//      } else {
//        val cleanImeiArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(cleanImeiArray _)
//        val cleanLtmArrayByImeiUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(cleanLtmArrayByImei _)
//        df.withColumn("ieid", cleanImeiArrayUDF(col("ieid")))
//          .withColumn("ieid_tm", cleanLtmArrayByImeiUDF(col("ieid"), col("ieid_tm")))
//          .withColumn("ieid_ltm", cleanLtmArrayByImeiUDF(col("ieid"), col("ieid_ltm")))
//      }
//    } else {
//      df
//    }
//
//    // 对imei字段进行重命名
//    val renamedDF = (if (param.isIeidTable) {
//      cleanedDF.withColumn(s"ieid_tm$renamedSuffix", array(col("update_time")))
//        .withColumn(s"ieid_ltm$renamedSuffix", array(col("update_time")))
//    } else {
//      cleanedDF.withColumnRenamed("ieid_tm", s"ieid_tm$renamedSuffix")
//        .withColumnRenamed("ieid_ltm", s"ieid_ltm$renamedSuffix")
//    }).withColumnRenamed("ieid", s"ieid$renamedSuffix")
//
//    val tf14 = withIeidColumnsCore(_: DataFrame, IEID14, param)
//    val tf15 = withIeidColumnsCore(_: DataFrame, IEID15, param)
//    val transformedDF = renamedDF.transform(tf14 andThen tf15)
//
//    // 如果输出有imei,但输入是其他类型的imei,则需要把imei字段处理成array类型
//    if (param.output.idTypesEnumSec.contains(IEID) && param.isIeidTable && param.inputIdTypeEnumSec != IEID) {
//      transformedDF.withColumn("ieid", array(col(s"ieid$renamedSuffix")))
//        .withColumn("ieid_tm", array(col("update_time")))
//        .withColumn("ieid_ltm", array(col("update_time")))
//        .drop(s"ieid$renamedSuffix")
//    } else {
//      transformedDF.withColumn("ieid", col(s"ieid$renamedSuffix"))
//        .withColumn("ieid_tm", col(s"ieid_tm$renamedSuffix"))
//        .withColumn("ieid_ltm", col(s"ieid_ltm$renamedSuffix"))
//        .drop(s"ieid$renamedSuffix", s"ieid_tm$renamedSuffix", s"ieid_ltm$renamedSuffix")
//    }
//  }

  // 会将原有的imei14字段覆盖掉,改成新的类型
//  def withIeidColumnsCore(df: DataFrame, deviceType: SecDeviceType, param: IdMappingV4Param): DataFrame = {
//    val tmp = df.withColumn(s"${deviceType}_tm", col(s"ieid_tm$renamedSuffix"))
//      .withColumn(s"${deviceType}_ltm", col(s"ieid_ltm$renamedSuffix"))
//
//    val substring14ArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(substring14Array _)
//    val substring15ArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(substring15Array _)
//
//    val tmpFilter = deviceType match {
//      case IEID14 if IEID14.equals(param.inputIdTypeEnumSec) =>
//        tmp.filter(length(col(s"ieid$renamedSuffix")) === 14)
//      case IEID15 if IEID15.equals(param.inputIdTypeEnumSec) =>
//        tmp.filter(length(col(s"ieid$renamedSuffix")) === 15)
//      case _ =>
//        tmp
//    }
//
//    val newCol = deviceType match {
//      case IEID14 if IEID14.equals(param.inputIdTypeEnumSec) =>
//        substring(col(s"ieid$renamedSuffix"), 0, 14)
//      case IEID14 if param.isIeidTable => array(substring(col(s"ieid$renamedSuffix"), 0, 14))
//      case IEID14 => substring14ArrayUDF(col(s"ieid$renamedSuffix"))
//      case IEID15 if IEID15.equals(param.inputIdTypeEnumSec) =>
//        substring(col(s"ieid$renamedSuffix"), 0, 15)
//      case IEID15 if param.isIeidTable => array(substring(col(s"ieid$renamedSuffix"), 0, 15))
//      case IEID15 => substring15ArrayUDF(col(s"ieid$renamedSuffix"))
//    }
//    tmpFilter.withColumn(s"$deviceType", newCol)
//  }

  def getSrcTable(spark: SparkSession, idType: SecDeviceType, param: IdMappingV4Param): String = {
    idType match {
      case PID => param.deviceMatch match {
        case 2 => PropUtils.HIVE_TABLE_DM_PID_MAPPING_BK_VIEW
        case _ => PropUtils.HIVE_TABLE_DM_PID_MAPPING_VIEW
      }
      case MCID => PropUtils.HIVE_TABLE_DM_MCID_MAPPING_VIEW
      case DEVICE => PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC_VIEW
      case IEID => PropUtils.HIVE_TABLE_DM_IEID_MAPPING_VIEW
      case IEID14 => PropUtils.HIVE_TABLE_DM_IEID_MAPPING_VIEW // 都走imei的表,不走imei14的表
      case IEID15 => PropUtils.HIVE_TABLE_DM_IEID_MAPPING_VIEW
      case IFID => PropUtils.HIVE_TABLE_DM_IFID_MAPPING_VIEW
      case ISID => PropUtils.HIVE_TABLE_DM_ISID_MAPPING_VIEW
      case SNID => PropUtils.HIVE_TABLE_DM_SNID_MAPPING_VIEW
      case OIID => PropUtils.HIVE_TABLE_DM_OIID_MAPPING_VIEW
    }
  }

  /**
   * 对mapping表做以下修改:
   * 1. 输入为pid的sha256时,需要将库里的数据换成sha256的
   * 2. 其他情况mapping表不变
   */
  def buildMappingTable(spark: SparkSession, idType: SecDeviceType, param: IdMappingV4Param): DataFrame = {
    import spark.implicits._
    val table = getSrcTable(spark, idType, param)
    val baseDF = spark.table(table)
    // 去掉不需要的列:
    // 全表的字段 - 输出字段 - 输入字段 = 要去掉的字段
    val notNeededColumns = baseDF.schema.fieldNames.filter(_ != "update_time") diff
      baseDF.schema.fieldNames.filter(
        fname => param.output.idTypesEnumSec.flatMap(outId =>
            Seq(s"$outId", s"$outId".split("_")(0))
              .flatMap(f => Seq(s"$f", s"${f}_tm", s"${f}_ltm"))
        ).contains(fname)) diff {
      if (param.inputEncrypt.encryptType == 1) {
        Seq(s"${param.inputIdTypeEnumSec}", s"${param.inputIdTypeEnumSec}_md5",
          s"${param.inputIdTypeEnumSec}".split("_")(0))
      } else Seq(s"${param.inputIdTypeEnumSec}", s"${param.inputIdTypeEnumSec}".split("_")(0))
    } diff {
      if (param.output.matchOrigIeid.get == 1 && param.output.idTypesEnumSec.toSet.contains(IEID)) {
        Seq(s"orig_ieid", s"orig_ieid_tm", s"orig_ieid_ltm")
      }
      else Seq.empty[String]
    } diff {
      if (param.deviceMatch == 2) Seq("bk_tm", "device_index") else Seq.empty[String]
    }
    print(s"not needed columns is ${notNeededColumns.mkString("\t")} \n")
    var cleanDF = baseDF.drop(notNeededColumns: _*)

    // 对数据进行变换
    cleanDF = if (param.inputEncrypt.isSha256 && PID == param.inputIdTypeEnumSec) {
      cleanDF.join(
        spark.table(PropUtils.HIVE_TABLE_DIM_PID_TRANSFORM_FULL_PAR_SEC).select(PID.toString, s"${PID}_mobfin"),
        Seq(PID.toString)).drop(PID.toString).withColumnRenamed(s"${PID}_mobfin", PID.toString)
    } else if (param.inputEncrypt.isSha256 && IEID == param.inputIdTypeEnumSec) {
      cleanDF.join(
        spark.sql(
          s"""
             |select sha256(ieid) ieid_mobfin, md5(ieid) ieid
             |from (
             |  select stack(2, ieid, substring(ieid, 1, 14)) ieid
             |  from (
             |    select ieid
             |    from ${PropUtils.HIVE_TABLE_IEID_FULL}
             |    where ieid is not null
             |  ) as a
             |) as b
            """.stripMargin).distinct(),
        Seq(IEID.toString)).drop(IEID.toString).withColumnRenamed(s"${IEID}_mobfin", IEID.toString)
    } else {
      cleanDF
    }

    // 如果输出是MD5则直接将字段名称加_md5后缀,对于pid需要特殊处理
    if (param.output.encrypt.isMd5) {
      param.output.idTypesEnumSec.diff(Seq(param.inputIdTypeEnumSec)).foreach { oid =>
        cleanDF = cleanDF.withColumn(s"${oid}_md5", col(s"$oid"))
      }
      cleanDF
    } else {
      cleanDF
    }
  }
}
