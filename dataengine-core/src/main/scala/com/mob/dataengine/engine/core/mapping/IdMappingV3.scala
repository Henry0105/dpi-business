package com.mob.dataengine.engine.core.mapping

import java.util.regex.Pattern

import com.mob.dataengine.commons.enums.DeviceType.{DEVICE, DeviceType, PHONE}
import com.mob.dataengine.commons.enums.{DeviceType, InputType, MacArray}
import com.mob.dataengine.commons.pojo.{MatchInfo, OutCnt}
import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}
import com.mob.dataengine.commons.traits.{TableTrait, UDFCollections}
import com.mob.dataengine.commons.utils.{FnHelper, PropUtils}
import com.mob.dataengine.commons.{DeviceSrcReader, Encrypt, JobCommon}
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, IdMappingV3Param}
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec

object IdMappingV3 extends BaseJob[IdMappingV3Param] {
  var idmappingV3Job: IdMappingV3Job = _

  /**
   * 入口方法
   */
  override def run(): Unit = {
    val spark = jobContext.spark
    import spark.implicits._

    // TODO 后期param多可以合并起来一起做
    jobContext.params.foreach(p => {
      println(p)
      idmappingV3Job = IdMappingV3Job(jobContext.spark, p, jobContext.jobCommon)
      // 主要业务逻辑
      idmappingV3Job.submit()
      // 返回运行状态
      if (jobContext.jobCommon.hasRPC()) {
        val matchInfo = idmappingV3Job.matchInfo
        RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort,
          s"2\u0001${p.output.uuid}\u0002${matchInfo.toJsonString()}")
      }
    })
  }
}

case class IdMappingV3Job(
                           spark: SparkSession,
                           param: IdMappingV3Param,
                           jobCommon: JobCommon
                         ) extends UDFCollections with TableTrait {
  val outputColPrefix = "out_"
  val renamedSuffix = "_renamed"
  val uniqueID = "tmp_uuid" // 用来表示每行数据唯一的id
  var matchInfo: MatchInfo = _
  var idCounts: Map[DeviceType, Long] = _
  var inputDF: DataFrame = _
  var matchInfoTmp: MatchInfoTmp = MatchInfoTmp()
  val inputEncrypt: Encrypt = param.inputEncrypt
  val inputEncryptUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(inputEncrypt.compute _)
  val hubService: DataHubService = DataHubServiceImpl(spark)
  val isBk: Boolean = param.deviceMatch == 2 && PHONE.equals(param.inputIdTypeEnum) &&
    param.output.idTypesEnum.contains(DEVICE)

  import DeviceType._
  import spark.implicits._

  var idCnt: Long = 0L
  var matchCnt: Long = 0L

  def submit(): Unit = {
    // prepare()
    val joinedDF = getJoinedDF().cache()
    // rightField可能为空，不统计
    matchCnt = joinedDF.filter(col(param.rightField).isNotNull).count()
    //    matchCnt = joinedDF.count()
    val tmpDF = if (param.output.idTypesEnum.contains(DEVICE) && param.deviceMatch.equals(1)
      && param.inputIdTypeEnum != DeviceType.DEVICE) {
      filterDeviceByTags(joinedDF)
    } else if (isBk) {
      backTrackMatch(joinedDF)
    } else {
      joinedDF
    }
    var unionDF = unionUnJoined(tmpDF.drop(uniqueID)).persist(StorageLevel.DISK_ONLY)
    unionDF = unionDF.repartition(FnHelper.getCoalesceNum(idCnt, 300000)).persist(StorageLevel.DISK_ONLY)
    stat(unionDF)
    persist(unionDF)
  }

  def unionUnJoined(joinedDF: DataFrame): DataFrame = {
    val nullFieldsList = joinedDF.schema.fieldNames
      .filter(name => name == param.rightField || name.startsWith("out")) diff inputDF.schema.fieldNames
    var notJoinedDF = broadcast(inputDF)
      .join(joinedDF.drop(inputDF.schema.fieldNames: _*),
        lower(inputDF(param.leftField)) === lower(joinedDF(param.rightField)), "left")
      .filter(joinedDF(param.rightField).isNull).drop(uniqueID)
    for (colName <- nullFieldsList) {
      if (param.output.idTypesEnum.contains(DEVICE) && param.deviceMatch.equals(1)
        && param.inputIdTypeEnum != DeviceType.DEVICE) {
        if (Set(s"${DeviceType.DEVICE}_ltm", s"out_${DeviceType.DEVICE}").contains(colName)) {
          notJoinedDF = notJoinedDF.drop(colName).withColumn(colName, array(lit("")))
        } else {}
      }
    }
    joinedDF.union(notJoinedDF)
  }

  /**
   * 对输入数据与mapping表进行处理
   * 然后对两者进行内连接
   *
   * @return inner join后的数据
   */
  def getJoinedDF(): DataFrame = {
    // 对输入数据的字段处理
    inputDF = addJoinField2DataFrame(buildInputDataFrame(spark), true)
    // 增加一个唯一id,为了确保 匹配device且deviceMatch = 1时，数据丢失问题
    inputDF = inputDF.withColumn(uniqueID, monotonically_increasing_id())
    // 根据inputIdTypeEnum选取合适的mapping表，并且进行字段处理
    val origMappingDF = addJoinField2DataFrame(buildMappingTable(spark, param.inputIdTypeEnum), false)
    var _transformDF = origMappingDF
    if (param.output.matchOrigImei.get == 1 && param.output.idTypesEnum.toSet.contains(IMEI)) {
      _transformDF = origMappingDF
        .drop(s"imei", s"imei_tm", s"imei_ltm", "imei_md5")
        .withColumnRenamed(s"orig_imei", s"imei")
        .withColumnRenamed(s"orig_imei_tm", s"imei_tm")
        .withColumnRenamed(s"orig_imei_ltm", s"imei_ltm")
    }
    val transformedDF = transformMappingTable(_transformDF)

    inputDF.cache()
    idCnt = inputDF.count()
    val joinCondition = if (param.inputIdTypeEnum != DEVICE && param.inputs.head.encrypt.encryptType == 1
      && !Set(IMEI14, IMEI15).contains(param.inputIdTypeEnum)) {
      lower(inputDF(param.leftField)) ===
        lower(coalesce(transformedDF(param.rightField), transformedDF(s"${param.inputIdTypeEnum}_md5")))
    } else {
      lower(inputDF(param.leftField)) === lower(transformedDF(param.rightField))
    }
    // 1kw以下走广播join
    if (idCnt > 10000000) { // 1kw
      inputDF.join(transformedDF, joinCondition, "inner")
    } else {
      broadcast(inputDF).join(transformedDF, joinCondition, "inner")
    }
  }

  def initTagsDF(): DataFrame = {
    spark.sql(
      s"""
         |SELECT device, size(tags) tags_size
         |FROM ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_VIEW}
         |WHERE tags is not null
       """.stripMargin)
      .toDF("device", "tags_size")
  }

  // 取标签最多的device
  def filterDeviceByTags(df: DataFrame): DataFrame = {
    var tmp = df
    val tagsDF = initTagsDF()
    val deviceColName = s"$outputColPrefix$DEVICE"
    val deviceDF = df.select(explode(col(deviceColName))).distinct().toDF(deviceColName)
    val deviceTagsSizeDF = deviceDF.join(tagsDF, deviceDF(deviceColName) === tagsDF("device"), "inner")
      .drop(tagsDF("device"))
      .toDF("device", "tags_size")
      .cache()

    spark.udf.register("zip_cols", (c1: Seq[String], c2: Seq[String]) => {
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
        row_number().over(Window.partitionBy(param.leftField, uniqueID)
          .orderBy(col("tags_size").desc).orderBy(deviceTagsSizeDF("device").desc)))
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

  def persist(joinedDF: DataFrame): Unit = {
    val resultDF = persist2Hive(joinedDF)
    export2Hdfs(resultDF)
  }

  def stat(df: DataFrame): Unit = {
    val matchInfoTmp = MatchInfoTmp(idCnt = idCnt, matchCnt = matchCnt)
    val hasMacField = MAC.equals(param.inputIdTypeEnum)
    println(s"match cnt ${matchInfoTmp.matchCnt}")

    if (hasMacField) {
      matchInfoTmp.iosUploadMac = getMacarrayMatchCnt(spark, df.select(param.leftField))
      matchInfoTmp.iosMatchMac = getMacarrayMatchCnt(spark,
        df.select(param.rightField).filter(col(param.rightField).isNotNull))
    }

    // 统计各个指标
    val fieldsStatMap = param.output.idTypesEnum.diff(Seq(param.inputIdTypeEnum)).map { f =>
      val outField = s"$outputColPrefix$f"
      (s"$f", statCol(spark, outField, df))
    }.toMap

    val outCnt = new OutCnt
    param.output.idTypesEnum.filterNot(_.equals(param.inputIdTypeEnum))
      .foreach(idEnum => outCnt.set(s"${idEnum}_cnt", fieldsStatMap(s"$idEnum")))

    matchInfo = MatchInfo(jobCommon.jobId, param.output.uuid, matchInfoTmp.idCnt, matchInfoTmp.matchCnt, outCnt)
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

  def statCol(spark: SparkSession, field: String, df: DataFrame): Long = {
    import spark.implicits._
    df.select(field).filter(col(field).isNotNull)
      .flatMap(_.getAs[Seq[String]](field)).distinct().count()
  }

  def persist2Hive(df: DataFrame): DataFrame = {
    val cleanMapUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(clean_map _)
    var tmp = df
    // 做出match_ids列
    val outCols: Seq[Column] = param.output.idTypesEnum.flatMap { c =>
      Seq(lit(c.id), concat_ws(",", col(s"$outputColPrefix$c")))
    }
    val outLtmCols = param.output.idTypesEnum.flatMap { c =>
      if (isBk || c.equals(param.inputIdTypeEnum) || param.imeiKeys.contains(c)) {
        Seq(lit(c.id + 100), col("update_time"))
      } else {
        Seq(lit(c.id + 100), concat_ws(",", col(s"${c}_ltm")))
      }
    }
    val newOutCols = outCols ++ outLtmCols
    tmp = tmp.withColumn("match_ids", cleanMapUDF(map(newOutCols: _*)))
    // .withColumn("match_ltms", cleanMapUDF(map(outLtmCols: _*)))

    // 做出id列
    tmp = tmp.withColumn("id", col(param.origJoinField))

    tmp.createOrReplaceTempView("tmp")

    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |partition(uuid='${param.output.uuid}', day='${jobCommon.day}')
         |select id, match_ids, ${param.inputIdTypeEnum.id} as id_type,
         |  ${param.inputEncrypt.encryptType} as encrypt_type, data
         |from tmp
        """.stripMargin)

    // todo 这里以后只写一个表 rp_data_hub
    persist2DataHub()

    tmp
  }

  def persist2DataHub(): Unit = {
    val id2Key = param.output.idTypes.map{ id =>
      val key = hubService.deduceIndex(id.toString, param.output.encrypt.encryptType.toString)
      (id, key)
    }.toMap

    val otherFields = param.output.idTypes.map { id =>
      // code, id, ltm
      s"""
         |'${id2Key(id)}',
         |if(match_ids[$id] is not null, array(match_ids[$id], match_ids[${id + 100}]), null)
        """.stripMargin
    }.mkString(",")

    param.inputs.map{ input =>
      sql(
        s"""
           |select feature
           |from ${PropUtils.HIVE_TABLE_DATA_HUB}
           |where uuid = '${input.uuid}'
          """.stripMargin)
    }.reduce(_ union _).createOrReplaceTempView("seed_tb")

    spark.udf.register("merge_feature", (resultMap: Map[String, Seq[String]],
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

    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_HUB} partition(uuid='${param.output.uuid}')
         |select merge_feature(map('seed', array(data), $otherFields), feature)
         |from tmp
         |left join (
         |  select seed, feature
         |  from (
         |    select feature['seed'][0] seed, feature,
         |      row_number() over (partition by feature['seed'][0] order by feature['seed'][0]) rn
         |    from seed_tb
         |  ) as a
         |  where rn = 1
         |) as b
         |on tmp.data = b.seed
        """.stripMargin)
  }

  def export2Hdfs(df: DataFrame): Unit = {
    df.createOrReplaceTempView("final_table")

    // 作出匹配的数据列和时间列
    val map2Fields = param.output.idTypesEnum.map { idType =>
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
    if (param.output.keepSeed.get == 0) {
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
           | ${if (param.output.limit > 0) s"limit ${param.output.limit}" else ""})
         """.stripMargin
      }.mkString(" union all ")

      spark.sql(tmpStr)
        .write.mode(SaveMode.Overwrite)
        .option("header", "false").option("sep", "\t")
        .option("emptyValue", null)
        .partitionBy("idType")
        .csv(param.output.hdfsOutput)
    } else {
      val resDF = spark.sql(
        s"""
           |select ${(idField :: map2Fields.map(_._2).toList).mkString(",")}
           |from final_table
           |${if (param.output.limit > 0) s"limit ${param.output.limit}" else ""}
        """.stripMargin)

      resDF.repartition(1).write.mode(SaveMode.Overwrite)
        .option("header", "true").option("sep", "\t")
        .option("emptyValue", null)
        .csv(param.output.hdfsOutput)
    }
  }

  /**
   * 给dataframe添加需要进行join的key 大小写/mac清理/加密处理
   *
   * @param srcDF   要处理的df
   * @param isInput 表示数据是不是input,因为input的列已经做过某种处理(例如: 加密)
   * @return 处理过后的df
   */
  def addJoinField2DataFrame(srcDF: DataFrame, isInput: Boolean): DataFrame = {
    val unifyJoinFieldUDF = unifyJoinField(isInput)
    val origJoinField = if (isInput) {
      param.origJoinField
    } else {
      s"${param.inputIdTypeEnum}"
    }
    val joinField = if (isInput) {
      param.leftField
    } else {
      param.rightField
    }
    srcDF.withColumn(joinField, unifyJoinFieldUDF(col(origJoinField)))
  }

  def unifyJoinField(isInput: Boolean): Column => Column = {
    val f1 = if (0 == param.inputs.head.encrypt.encryptType) {
      param.inputIdTypeEnum match {
        case MAC =>
          val cleanMacUDF = regexp_replace(_: Column, ":", "")
          cleanMacUDF andThen lower
        case IDFA =>
          val cleanIDFAUDF = regexp_replace(_: Column, "-", "")
          cleanIDFAUDF andThen lower
        case _ => lower _
      }
    } else if (IDFA == param.inputIdTypeEnum && 1 == param.inputs.head.encrypt.encryptType) {
      identity[Column] _
    } else {
      lower _
    }

    if (isInput) {
      f1
    } else { // 如果是mapping表,需要进行加密处理
      col: Column => inputEncryptUDF(f1(col))
    }
  }

  def buildInputDataFrame(spark: SparkSession): DataFrame = {
    val srcDF = param.inputs.map(input =>
      DeviceSrcReader.toDFV2(spark, input, param.trackDay, param.trackDayIndex, isBk, hubService)).reduce(_ union _)
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
  def transformMappingTable(df: DataFrame): DataFrame = {
    val latestArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(latestArray _)
    val latestArrayLtmUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(latestLtmArray _)
    val outputEncrypt: Encrypt = param.output.encrypt
    val outputEncryptUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(param.output.encrypt.compute _)
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

    val outputColsEnum = param.output.idTypesEnum.diff(Seq(param.inputIdTypeEnum))
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

    // 如果要输出列中包含输入类型,即: 输入mac,输出mac的MD5等
    if (param.output.idTypesEnum.contains(param.inputIdTypeEnum)) {
      val transCol = if (param.output.encrypt.encryptType != 1 || param.inputIdTypeEnum == DEVICE) {
        outputEncryptUDF(col(s"${param.inputIdTypeEnum}"))
      } else {
        col(s"${param.inputIdTypeEnum}_md5")
      }
      tmpDF = tmpDF.withColumn(s"$outputColPrefix${param.inputIdTypeEnum}", transCol)
    }

    tmpDF
  }

  /**
   * 给mapping表加imei14/imei15两列,分2中情况
   * 1. mapping表是imei_mapping
   * 2. 其他mapping表
   */
  def withImeiColumns(df: DataFrame): DataFrame = {
    val cleanedDF = if (1 == param.cleanImei) {
      if (param.isImeiTable) {
        df.filter("imei rlike '^[0-9]+$' ")
      } else {
        val cleanImeiArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(cleanImeiArray _)
        val cleanLtmArrayByImeiUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(cleanLtmArrayByImei _)
        df.withColumn("imei", cleanImeiArrayUDF(col("imei")))
          .withColumn("imei_tm", cleanLtmArrayByImeiUDF(col("imei"), col("imei_tm")))
          .withColumn("imei_ltm", cleanLtmArrayByImeiUDF(col("imei"), col("imei_ltm")))
      }
    } else {
      df
    }

    // 对imei字段进行重命名
    val renamedDF = (if (param.isImeiTable) {
      cleanedDF.withColumn(s"imei_tm$renamedSuffix", array(col("update_time")))
        .withColumn(s"imei_ltm$renamedSuffix", array(col("update_time")))
    } else {
      cleanedDF.withColumnRenamed("imei_tm", s"imei_tm$renamedSuffix")
        .withColumnRenamed("imei_ltm", s"imei_ltm$renamedSuffix")
    }).withColumnRenamed("imei", s"imei$renamedSuffix")

    val tf14 = withImeiColumnsCore(_: DataFrame, IMEI14)
    val tf15 = withImeiColumnsCore(_: DataFrame, IMEI15)
    val transformedDF = renamedDF.transform(tf14 andThen tf15)

    // 如果输出有imei,但输入是其他类型的imei,则需要把imei字段处理成array类型
    if (param.output.idTypesEnum.contains(IMEI) && param.isImeiTable && param.inputIdTypeEnum != IMEI) {
      transformedDF.withColumn("imei", array(col(s"imei$renamedSuffix")))
        .withColumn("imei_tm", array(col("update_time")))
        .withColumn("imei_ltm", array(col("update_time")))
        .drop(s"imei$renamedSuffix")
    } else {
      transformedDF.withColumn("imei", col(s"imei$renamedSuffix"))
        .withColumn("imei_tm", col(s"imei_tm$renamedSuffix"))
        .withColumn("imei_ltm", col(s"imei_ltm$renamedSuffix"))
        .drop(s"imei$renamedSuffix", s"imei_tm$renamedSuffix", s"imei_ltm$renamedSuffix")
    }
  }

  // 会将原有的imei14字段覆盖掉,改成新的类型
  def withImeiColumnsCore(df: DataFrame, deviceType: DeviceType): DataFrame = {
    val tmp = df.withColumn(s"${deviceType}_tm", col(s"imei_tm$renamedSuffix"))
      .withColumn(s"${deviceType}_ltm", col(s"imei_ltm$renamedSuffix"))

    val substring14ArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(substring14Array _)
    val substring15ArrayUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(substring15Array _)

    val tmpFilter = deviceType match {
      case IMEI14 if IMEI14.equals(param.inputIdTypeEnum) =>
        tmp.filter(length(col(s"imei$renamedSuffix")) === 14)
      case IMEI15 if IMEI15.equals(param.inputIdTypeEnum) =>
        tmp.filter(length(col(s"imei$renamedSuffix")) === 15)
      case _ =>
        tmp
    }

    val newCol = deviceType match {
      case IMEI14 if IMEI14.equals(param.inputIdTypeEnum) =>
        substring(col(s"imei$renamedSuffix"), 0, 14)
      case IMEI14 if param.isImeiTable => array(substring(col(s"imei$renamedSuffix"), 0, 14))
      case IMEI14 => substring14ArrayUDF(col(s"imei$renamedSuffix"))
      case IMEI15 if IMEI15.equals(param.inputIdTypeEnum) =>
        substring(col(s"imei$renamedSuffix"), 0, 15)
      case IMEI15 if param.isImeiTable => array(substring(col(s"imei$renamedSuffix"), 0, 15))
      case IMEI15 => substring15ArrayUDF(col(s"imei$renamedSuffix"))
    }
    tmpFilter.withColumn(s"$deviceType", newCol)
  }

  def getSrcTable(spark: SparkSession, idType: DeviceType): String = {
    idType match {
      case PHONE => param.deviceMatch match {
        case 2 => PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK_VIEW
        case _ => PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_VIEW
      }
      case MAC => PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW
      case DEVICE => PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3_VIEW
      case IMEI => PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3_VIEW
      case IMEI14 => PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3_VIEW // 都走imei的表,不走imei14的表
      case IMEI15 => PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3_VIEW
      case IDFA => PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3_VIEW
      case IMSI => PropUtils.HIVE_TABLE_DM_IMSI_MAPPING_V3_VIEW
      case SERIALNO => PropUtils.HIVE_TABLE_DM_SERIALNO_MAPPING_V3_VIEW
      case OAID => PropUtils.HIVE_TABLE_DM_OAID_MAPPING_V3_VIEW
    }
  }

  def buildMappingTable(spark: SparkSession, idType: DeviceType): DataFrame = {
    val table = getSrcTable(spark, idType)
    val baseDF = spark.table(table)
    // 去掉不需要的列:
    // 全表的字段 - 输出字段 - 输入字段 = 要去掉的字段
    val notNeededColumns = baseDF.schema.fieldNames.filter(_ != "update_time") diff
      baseDF.schema.fieldNames.filter(
        fname => param.output.idTypesEnum.flatMap(outId =>
          if (param.output.encrypt.encryptType == 1) { // 源表里面有md5字段的需要保留
            Seq(s"$outId", s"$outId".split("_")(0)).flatMap(f =>
              Seq(s"$f", s"${f}_md5", s"${f}_tm", s"${f}_ltm"))
          } else {
            Seq(s"$outId", s"$outId".split("_")(0))
              .flatMap(f => Seq(s"$f", s"${f}_tm", s"${f}_ltm"))
          }
        ).contains(fname)) diff {
      if (param.inputEncrypt.encryptType == 1) {
        Seq(s"${param.inputIdTypeEnum}", s"${param.inputIdTypeEnum}_md5",
          s"${param.inputIdTypeEnum}".split("_")(0))
      } else Seq(s"${param.inputIdTypeEnum}", s"${param.inputIdTypeEnum}".split("_")(0))
    } diff {
      if (param.output.matchOrigImei.get == 1 && param.output.idTypesEnum.toSet.contains(IMEI)) {
        Seq(s"orig_imei", s"orig_imei_tm", s"orig_imei_ltm")
      }
      else Seq.empty[String]
    } diff {
      if (param.deviceMatch == 2) Seq("bk_tm", "device_index") else Seq.empty[String]
    }
    print(s"not needed columns is ${notNeededColumns.mkString("\t")} \n")
    if ((idType.equals(IDFA) || idType.equals(IMSI) || (param.output.idTypesEnum.toSet
      intersect Set(IMEI15, IMEI, IMEI14)).isEmpty) && !param.isImeiTable) {
      baseDF.drop(notNeededColumns: _*)
    } else {
      withImeiColumns(baseDF.drop(notNeededColumns: _*))
    }
  }

}
