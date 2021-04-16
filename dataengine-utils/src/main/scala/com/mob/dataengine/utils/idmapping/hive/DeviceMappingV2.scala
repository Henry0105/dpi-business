package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType

import scala.collection.mutable.ArrayBuffer

/**
 * @author juntao zhang
 */
class DeviceMappingV2(
  @transient spark: SparkSession, day: String,
  test: Boolean = false, numRepartions: Int = 1024) extends Serializable {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  /* 原始表数据量: 4209318222 */
  val androidTable: String = PropUtils.HIVE_ORIGINAL_ANDROID_ID_MAPPING_V2
  /* 原始表数据量: 4209318222 */
  val iosTable: String = PropUtils.HIVE_ORIGINAL_IOS_ID_MAPPING_V2
  val targetTable: String = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3
  val duidTable: String = PropUtils.HIVE_TABLE_DEVICE_DUID_MAPPING_NEW
  val targetTable_inc: String = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3_INC
  /**
   * 数据量: 41,9735,1534  196.2 G
   * 1024 * 196M
   */
  val android = "1"
  /**
   * 数据量: 41,0353,8139 116.3 G
   * 1024 * 116M
   * todo 分区改成512
   */
  val ios = "2"

  spark.udf.register("aggregateDevice", new AggregateIdTypeV2(200))

  spark.udf.register("splitIds", (ids: String) => {
    if (StringUtils.isBlank(ids)) {
      null
    } else {
      val tmp = (if (ids.endsWith(",")) ids.dropRight(1) else ids).split(",").map(_.trim)
        .filter(_.nonEmpty)
      if (tmp.nonEmpty) {
        tmp
      } else {
        null
      }
    }
  })

  spark.udf.register("updateTime",
    (macTm: Seq[String], imeiTm: Seq[String], idfaTm: Seq[String],
      phoneTm: Seq[String], imsiTm: Seq[String], serialnoTm: Seq[String],
     origImeiTm: Seq[String], oaidTm: Seq[String]) => {
      val seq = Seq(macTm, imeiTm, idfaTm, phoneTm, imsiTm, serialnoTm, origImeiTm, oaidTm).filter(arr => {
        arr != null
      }).flatten
      if (seq.isEmpty) {
        null
      } else {
        seq.max
      }
    }
  )

  spark.udf.register("tmNorm",
    (macTm: Seq[String], imeiTm: Seq[String], idfaTm: Seq[String],
      phoneTm: Seq[String], imsiTm: Seq[String], serialnoTm: Seq[String],
     origImeiTm: Seq[String], oaidTm: Seq[String]) => {
      Seq(macTm, imeiTm, idfaTm, phoneTm, imsiTm, serialnoTm, origImeiTm, oaidTm).filter(arr => {
        arr != null
      }).flatten.nonEmpty
    }
  )

  spark.udf.register("isNorm", (device: String) => {
    StringUtils.isNotBlank(device) && device.matches("[a-f0-9]{40}") &&
      !device.equals("0000000000000000000000000000000000000000")
  })

  def sql(sql: String): DataFrame = {
    logger.info("\n>>>>>>>>>>>>>>>>>")
    logger.info(sql)
    val df = spark.sql(sql)
    logger.info("<<<<<<<<<<<<<<\n\n")
    df
  }


  def persistDeviceMapping(day: String, plat: String, dataFrame: DataFrame): Unit = {
    val id = s"temp_${plat}_$day"
    dataFrame.createOrReplaceTempView(id)
    var dataset = sql(
      s"""
         | select
         |  ${dataFrame.schema.map(_.name).mkString(",")},
         |  updateTime(
         |  mac_ltm,imei_ltm,idfa_ltm,phone_ltm,imsi_ltm,serialno_ltm,orig_imei_ltm,oaid_ltm
         |  ) as update_time
         |  from $id where tmNorm(
         |  mac_ltm,imei_ltm,idfa_ltm,phone_ltm,imsi_ltm,serialno_ltm,orig_imei_ltm,oaid_ltm
         |  )
      """.stripMargin)

    // 测试环境取部分数据
    if (test) {
      dataset = dataset.filter(r => {
        val a = r.getString(0).charAt(39)
        a == '0' || a == '6' || a == 'b' // sample
      })
    }

    val viewName = s"tmp_2_${day}_$plat"
    dataset.createOrReplaceTempView(viewName)

    val fields = spark.table(targetTable).schema
      .filterNot(sf => Set("day", "plat").contains(sf.name))
      .map{ sf => if (sf.dataType.isInstanceOf[ArrayType]) {
        s"if(size(${sf.name}) < 1, null, ${sf.name}) as ${sf.name}"
      } else {
        sf.name
      }}

    sql(
      s"""
         |from $viewName
         |insert overwrite table $targetTable partition(day='$day',plat='$plat')
         |select ${fields.mkString(",")}
         |insert overwrite table $targetTable_inc partition(day='$day',plat='$plat')
         |select ${fields.mkString(",")}
         |where update_time >= ${DateUtils.getDayTimeStamp(day, -2)}
      """.stripMargin)


  }

  def duidMapping(plat: String): String = {
    val tb = s"duid_mapping_$plat"
    sql(
      s"""
        |select device, split(a.agg_duid.device, ",") as duid
        |from (
        |  select device, aggregateDevice(duid, '', '') as agg_duid
        |  from $duidTable
        |  where plat = $plat and duid is not null and duid <> '' and duid <> 'null'
        |  group by device
        |) as a
      """.stripMargin).createOrReplaceTempView(tb)
    tb
  }

  def trimAndExtract(zippedField: String, key: String, len: Int): String = {
    val keyAccessor = s"tuple.$key"
    s"""
       |filter(
       |  transform(
       |    $zippedField, tuple ->
       |    if(length($keyAccessor) < $len,
       |      null,
       |      if(length($keyAccessor) = $len,
       |        tuple,
       |        named_struct(
       |          '$key', substring($keyAccessor, 0, $len),
       |          '${key}_tm', tuple.imei_tm,
       |          '${key}_ltm', tuple.imei_ltm
       |        )
       |      )
       |    )
       |  ),
       |  tuple -> length($keyAccessor) >= $len
       |)
     """.stripMargin
  }

  def run(): Unit = {
    val androidDuidMapping = duidMapping(android)
    // 对imei字段进行扩充,将imei14/imei15都加入其中
    import spark.implicits._
    // 由于会有1对多的问题,需要去掉太多的数据,在此设置一个阈值
    val one2ManyThreshold: Int = 10000

    val androidDF = sql(
      s"""
         |select
         |  android_mapping.device,
         |  splitIds(imei) as imei,
         |  splitIds(imei_tm) as imei_tm,
         |  null as idfa,
         |  splitIds(imei_ltm) as imei_ltm,
         |  null as idfa_tm,
         |  null as idfa_ltm,
         |  splitIds(mac) as mac,
         |  splitIds(mac_tm) as mac_tm,
         |  splitIds(mac_ltm) as mac_ltm,
         |  splitIds(phone) as phone,
         |  splitIds(phone_tm) as phone_tm,
         |  splitIds(phone_ltm) as phone_ltm,
         |  splitIds(imsi) as imsi,
         |  splitIds(imsi_tm) as imsi_tm,
         |  splitIds(imsi_ltm) as imsi_ltm,
         |  duid,
         |  splitIds(serialno) as serialno,
         |  splitIds(serialno_tm) as serialno_tm,
         |  splitIds(serialno_ltm) as serialno_ltm,
         |  splitIds(orig_imei) as orig_imei,
         |  splitIds(orig_imei_tm) as orig_imei_tm,
         |  splitIds(orig_imei_ltm) as orig_imei_ltm,
         |  splitIds(oaid) as oaid,
         |  splitIds(oaid_tm) as oaid_tm,
         |  splitIds(oaid_ltm) as oaid_ltm
         |from $androidTable as android_mapping
         |left join $androidDuidMapping as duid_mapping
         |on android_mapping.device = duid_mapping.device
         |  where isNorm(android_mapping.device)
      """.stripMargin)
      .withColumn("imei_zipped", arrays_zip($"imei", $"imei_tm", $"imei_ltm"))
        .withColumn("imei14_struct", expr(trimAndExtract("imei_zipped", "imei", 14)))
        .withColumn("imei15_struct", expr(trimAndExtract("imei_zipped", "imei", 15)))
        .withColumn("imei_14", expr("transform(imei14_struct, tuple -> tuple.imei)"))
        .withColumn("imei_14_tm", expr("transform(imei14_struct, tuple -> tuple.imei_tm)"))
        .withColumn("imei_14_ltm", expr("transform(imei14_struct, tuple -> tuple.imei_ltm)"))
        .withColumn("imei_15", expr("transform(imei15_struct, tuple -> tuple.imei)"))
        .withColumn("imei_15_tm", expr("transform(imei15_struct, tuple -> tuple.imei_tm)"))
        .withColumn("imei_15_ltm", expr("transform(imei15_struct, tuple -> tuple.imei_ltm)"))
        .drop("imei_struct", "imei14_struct", "imei15_struct")
        .filter(size(col("imei")) <= one2ManyThreshold)
      .filter(size(col("mac")) <= one2ManyThreshold)
      .filter(size(col("imsi")) <= one2ManyThreshold)
      .filter(size(col("serialno")) <= one2ManyThreshold)


    persistDeviceMapping(day, android, androidDF)

    val iosDuidMapping = duidMapping(ios)
    persistDeviceMapping(day, ios, sql(
      s"""
         |select
         |  ios_mapping.device,
         |  null as imei,
         |  null as imei_14,
         |  null as imei_15,
         |  null as imei_tm,
         |  null as imei_ltm,
         |  null as imei_14_tm,
         |  null as imei_14_ltm,
         |  null as imei_15_tm,
         |  null as imei_15_ltm,
         |  splitIds(idfa) as idfa,
         |  splitIds(idfa_tm) as idfa_tm,
         |  splitIds(idfa_ltm) as idfa_ltm,
         |  splitIds(mac) as mac,
         |  splitIds(mac_tm) as mac_tm,
         |  splitIds(mac_ltm) as mac_ltm,
         |  splitIds(phone) as phone,
         |  splitIds(phone_tm) as phone_tm,
         |  splitIds(phone_ltm) as phone_ltm,
         |  null as imsi,
         |  null as imsi_tm,
         |  null as imsi_ltm,
         |  duid,
         |  null as serialno,
         |  null as serialno_tm,
         |  null as serialno_ltm,
         |  null as orig_imei,
         |  null as orig_imei_tm,
         |  null as orig_imei_ltm,
         |  null as oaid,
         |  null as oaid_tm,
         |  null as oaid_ltm
         |from $iosTable as ios_mapping
         |left join $iosDuidMapping as duid_mapping
         |on ios_mapping.device = duid_mapping.device
         |  where isNorm(ios_mapping.device)
      """.stripMargin)
      .filter(size(col("mac")) <= one2ManyThreshold))
  }
}
