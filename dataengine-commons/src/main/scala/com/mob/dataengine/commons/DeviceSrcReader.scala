package com.mob.dataengine.commons

import com.mob.dataengine.commons.enums.{DeviceType, InputType}
import com.mob.dataengine.commons.pojo.ParamInput
import com.mob.dataengine.commons.service.DataHubService
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.core.constants.DataengineExceptionType
import com.mob.dataengine.core.utils.DataengineExceptionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @author juntao zhang
 */
object DeviceSrcReader {

  def hasPar(spark: SparkSession, table: String, partition: String): Boolean = {
    spark.sql(s"show partitions $table")
      .collect()
      .map(_.getAs[String](0).split("/")(2))
      .exists(_.equals(partition))
  }

  def hasPar(spark: SparkSession, table: String, filterBy: String => Boolean = _ => true): Boolean = {
    spark.sql(s"show partitions $table").collect().map(_.getString(0)).exists(filterBy)
  }

  @deprecated
  def toRDD(spark: SparkSession, input: ParamInput): RDD[String] = {
    if (input.isHiveCache) {
      val hasUUIDPartition: Boolean = hasPar(spark,
        PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
        s"uuid=${input.getUUID}")
      if (hasUUIDPartition) {
        spark.sql(
          s"""
             |select split(data, '\u0001')[0] device
             |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
             |where uuid='${input.value}'
             |and data is not null
             |and split(data, '\u0001')[0] <> '-1'
          """.stripMargin
        ).rdd.map { case Row(device: String) => device }.distinct()
      } else {
        spark.sql(
          s"""
             |select device
             |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
             |lateral view explode(split(match_ids[${input.idType.getOrElse(DeviceType.DEVICE.id)}], ',')) t as device
             |where uuid = '${input.value}'
             |and match_ids[${input.idType.getOrElse(DeviceType.DEVICE.id)}] is not null
          """.stripMargin
        ).rdd.map { case Row(device: String) => device }.distinct()
      }
    } else {
      SrcURLReader.toRDD(spark, input.value, input.compressType.getOrElse("none"))
    }.filter(StringUtils.isNotBlank)
  }

  @deprecated
  def toDeviceRDD(spark: SparkSession, input: ParamInput): RDD[String] = {
    val numPattern = "[0-9|a-f]{40}".r
    toRDD(spark, input).filter(device => {
      StringUtils.isNotBlank(device) && numPattern.pattern.matcher(device).find()
    })
  }

  @deprecated
  def toDeviceRDD2(spark: SparkSession, input: JobInput): RDD[String] = {
    val numPattern = "[0-9|a-f]{40}".r
    toRDD2(spark, input).filter(device => {
      StringUtils.isNotBlank(device) && numPattern.pattern.matcher(device).find()
    })
  }

  @deprecated
  def toRDD2(spark: SparkSession, input: InputTrait): RDD[String] = {
    val hasUUIDPartition: Boolean = hasPar(spark,
      PropUtils.HIVE_TABLE_DATA_OPT_CACHE, s"uuid=${input.uuid}")
    println(s"hasUUIDPartition=>$hasUUIDPartition. uuid=>${input.uuid}")
    if (hasUUIDPartition) {
      spark.sql(
        s"""
           |select split(data, '\u0001')[0] device
           |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
           |where uuid='${input.uuid}'
           |and data is not null
           |and split(data, '\u0001')[0] <> '-1'
         """.stripMargin
      ).rdd.map { case Row(device: String) => device }
    } else if (hasPar(spark, PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, s => s.endsWith(s"uuid=${input.uuid}"))) {
      spark.sql(
        s"""
           |select device
           |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
           |lateral view explode(split(match_ids[${input.idType}], ',')) t as device
           |where uuid = '${input.uuid}'
           |and match_ids[${input.idType}] is not null
         """.stripMargin
      ).rdd.map { case Row(device: String) => device }
    } else {
      // todo 后面都走hubService
      // 此时数据在rp_data_hub里面, 这里只读取种子数据,其他的字段等后面落表的时候再join进来处理
      spark.sql(
        s"""
           |select feature['seed'][0]
           |from ${PropUtils.HIVE_TABLE_DATA_HUB}
           |where feature['seed'] is not null and uuid = '${input.uuid}'
          """.stripMargin)
        .rdd.map { case Row(device: String) => device }
    }
  }

  /** 首先检查uuid是否为sql,如果是走sql查询,如果不是去表中查询 */
  @deprecated
  def toDF(spark: SparkSession, input: JobInput): DataFrame = {
    val inputType = input.inputTypeEnum
    if (InputType.isSql(inputType)) {
      // sql的处理
      if (input.idx.isEmpty) {
        throw DataengineExceptionUtils("输入格式为sql,没有传参数idx", DataengineExceptionType.PARAMETER_ERROR)
      }
      val df = spark.sql(s"""${input.uuid.replaceAll("@", "'")}""")
      val fields = df.schema.fields
      val deviceCol = fields(input.idx.get - 1).name
      val allCols = fields.map(_.name)
      df.selectExpr(s"cast($deviceCol as string) as device",
        s"concat_ws('${input.sep.get}', ${allCols.mkString(",")}) as data")
    } else {
      // 正常uuid的处理
      val hasUUIDPartition: Boolean = hasPar(spark,
        PropUtils.HIVE_TABLE_DATA_OPT_CACHE, s"uuid=${input.uuid}")
      println(s"hasUUIDPartition=>$hasUUIDPartition. uuid=>${input.uuid}")
      if (hasUUIDPartition) {
        spark.sql(
          s"""
             |select split(data, '\u0001')[0] device, data
             |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
             |where uuid='${input.uuid}'
             |and data is not null
             |and split(data, '\u0001')[0] <> '-1'
           """.stripMargin)
      } else if (hasPar(spark, PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, s => s.endsWith(s"uuid=${input.uuid}"))) {
        spark.sql(
          s"""
             |select device, data
             |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
             |lateral view explode(split(match_ids[${input.idType}], ',')) t as device
             |where uuid = '${input.uuid}'
             |and match_ids[${input.idType}] is not null
           """.stripMargin)
      } else {
        // todo 后面都走hubService
        // 此时数据在rp_data_hub里面, 这里只读取种子数据,其他的字段等后面落表的时候再join进来处理
        spark.sql(
          s"""
             |select feature['4'][0], feature['seed'][0]
             |from ${PropUtils.HIVE_TABLE_DATA_HUB}
             |where feature['seed'] is not null and uuid = '${input.uuid}'
          """.stripMargin)
      }
    }.toDF("device", "data")
  }

  @deprecated
  def toRDDBT(spark: SparkSession, input: JobInput,
              trackDay: Option[String], trackDayIndex: Option[Int]): RDD[(String, String)] = {
    val hasTrackDay = trackDay.nonEmpty
    val hasUUIDPartition: Boolean = hasPar(spark,
      PropUtils.HIVE_TABLE_DATA_OPT_CACHE, s"uuid=${input.uuid}")
    println(s"hasUUIDPartition=>$hasUUIDPartition. uuid=>${input.uuid}")
    val index = trackDayIndex.get - 1

    if (hasUUIDPartition && hasTrackDay) {
      spark.sql(
        s"""
           |select split(data, '\u0001')[0] device
           |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
           |where uuid='${input.uuid}'
           |and data is not null
           |and split(data, '\u0001')[0] <> '-1'
         """.stripMargin
      ).rdd.map { case Row(device: String) => (device, trackDay.get) }
    } else {
      val filterData = if (hasTrackDay) "" else " and data is not null"
      val dayString = if (hasTrackDay) s"'${trackDay.get}'" else s" split(data, ',')[$index] "
      spark.sql(
        s"""
           |select device, $dayString as day
           |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
           |lateral view explode(split(match_ids[${DeviceType.DEVICE.id}], ',')) t as device
           |where uuid = '${input.uuid}'
           |and match_ids[${DeviceType.DEVICE.id}] is not null $filterData
         """.stripMargin
      ).rdd.map { case Row(device: String, day: String) => (device, day) }
    }
  }

  /**
   * 对种子数据做处理
   * 统一处理成以下schema
   * id, day[可选], data
   * data: 种子包
   * day: 指定的回溯日期
   * id: 从种子包/上次任务里取出来的id
   * @param trackDay: 统一的回溯日期
   * @param trackDayIndex: 回溯日期在种子包里面的时候,需要指定列号,从1开始
   * @param isBk: 表示是否是回溯种子数据
   */

  def toDFV2(spark: SparkSession, input: JobInput, trackDay: Option[String] = None,
           trackDayIndex: Option[Int] = None, isBk: Boolean = false, hubService: DataHubService): DataFrame = {
    val inputType = input.inputTypeEnum
    lazy val index = trackDayIndex.get - 1
    val hasTrackDay = trackDay.nonEmpty

    if (InputType.isSql(inputType)) {
      // sql的处理
      if (input.idx.isEmpty || input.sep.isEmpty) {
        throw DataengineExceptionUtils("ERROR: 输入格式为sql,必须传参数idx,sep",
          DataengineExceptionType.PARAMETER_ERROR)
      }
      val df = spark.sql(s"""${input.uuid.replaceAll("@", "'")}""")
      val prefix = "in_"
      val fields = df.schema.fieldNames
      import org.apache.spark.sql.functions._
      val columns = fields.map(name => col(name).as(s"$prefix$name"))
      val df2 = df.select(columns: _*)
      val fields2 = df2.schema.fieldNames
      val deviceCol = fields2(input.idx.get - 1)
      val normDF = df2.withColumn("id", col(deviceCol).cast("string"))
        .withColumn("data", concat_ws(input.sep.get, fields2.map(col): _ *))

      if (!isBk) {
        normDF
      } else {
        normDF.withColumn("day",
          {if (hasTrackDay) lit(trackDay.get) else col(fields2(index))}.cast("string"))
      }
    } else {
      val idx = input.idx.getOrElse(1)
      // 正常uuid的处理
      val hasUUIDPartition: Boolean = hasPar(spark,
        PropUtils.HIVE_TABLE_DATA_OPT_CACHE, s"uuid=${input.uuid}")
      println(s"hasUUIDPartition=>$hasUUIDPartition. uuid=>${input.uuid}")

      // 这里先将数据都准备成 device, data的形式
      val preDF =
        if (hasUUIDPartition) {
          spark.sql(
            s"""
               |select split(data, '${input.sep.getOrElse("\u0001")}')[${idx - 1}] id, data
               |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
               |where uuid='${input.uuid}'
               |and data is not null
               |and split(data, '\u0001')[0] <> '-1'
             """.stripMargin)
        } else if (hasPar(spark, PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, s => s.endsWith(s"uuid=${input.uuid}"))) {
          spark.sql(
            s"""
               |select device as id, data
               |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
               |lateral view explode(split(match_ids[${input.idType}], ',')) t as device
               |where uuid = '${input.uuid}'
               |and match_ids[${input.idType}] is not null
             """.stripMargin)
        } else {
          // todo 后面都走hubService
          // 此时数据在rp_data_hub里面, 这里只读取种子数据,其他的字段等后面落表的时候再join进来处理
          // todo 这里暂时使用不加密
          val seedSchema = SeedSchema(InputType.withName(input.inputType), input.idType, input.sep.getOrElse(","),
            Seq.empty[String], idx, input.uuid, input.encrypt)

          // 将出来的数据进行处理
          hubService.readSeed(seedSchema)
            .withColumn("data", col("feature").getField("seed").getItem(0))
            .drop("feature")
        }

      // 这里考虑是否带回溯日期的情况
      if (isBk) {
        val dayCol = if (hasTrackDay) {
          lit(trackDay.get)
        } else {
          split(col("data"), input.sep.getOrElse(",")).getItem(index)
        }
        preDF.withColumn("day", dayCol)
      } else {
        preDF
      }
    }
  }
}
