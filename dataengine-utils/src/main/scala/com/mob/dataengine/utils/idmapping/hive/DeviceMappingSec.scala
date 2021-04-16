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
class DeviceMappingSec(
  @transient spark: SparkSession, day: String,
  test: Boolean = false, numRepartions: Int = 1024) extends Serializable {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  /* 原始表数据量: 4209318222 */
  val androidTable: String = PropUtils.HIVE_ORIGINAL_ANDROID_ID_MAPPING_SEC
  /* 原始表数据量: 4209318222 */
  val iosTable: String = PropUtils.HIVE_ORIGINAL_IOS_ID_MAPPING_SEC
  val targetTable: String = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC
  val duidTable: String = PropUtils.HIVE_TABLE_DEVICE_DUID_MAPPING_NEW
  val targetTable_inc: String = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC_INC
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
    (mcidTm: Seq[String], ieidTm: Seq[String], ifidTm: Seq[String],
      pidTm: Seq[String], isidTm: Seq[String], snidTm: Seq[String],
     origIeidTm: Seq[String], oiidTm: Seq[String]) => {
      val seq = Seq(mcidTm, ieidTm, ifidTm, pidTm, isidTm, snidTm, origIeidTm, oiidTm).filter(arr => {
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
    (mcidTm: Seq[String], ieidTm: Seq[String], ifidTm: Seq[String],
      pidTm: Seq[String], isidTm: Seq[String], snidTm: Seq[String],
     origIeidTm: Seq[String], oiidTm: Seq[String]) => {
      Seq(mcidTm, ieidTm, ifidTm, pidTm, isidTm, snidTm, origIeidTm, oiidTm).filter(arr => {
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
         |  mcid_ltm,ieid_ltm,ifid_ltm,pid_ltm,isid_ltm,snid_ltm,orig_ieid_ltm,oiid_ltm
         |  ) as update_time
         |  from $id where tmNorm(
         |  mcid_ltm,ieid_ltm,ifid_ltm,pid_ltm,isid_ltm,snid_ltm,orig_ieid_ltm,oiid_ltm
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
       |          '${key}_tm', tuple.ieid_tm,
       |          '${key}_ltm', tuple.ieid_ltm
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
    // 对ieid字段进行扩充,将ieid14/ieid15都加入其中
    import spark.implicits._
    // 由于会有1对多的问题,需要去掉太多的数据,在此设置一个阈值
    val one2ManyThreshold: Int = 10000

    val androidDF = sql(
      s"""
         |select
         |  android_mapping.device,
         |  splitIds(ieid) as ieid,
         |  splitIds(ieid_tm) as ieid_tm,
         |  null as ifid,
         |  splitIds(ieid_ltm) as ieid_ltm,
         |  null as ifid_tm,
         |  null as ifid_ltm,
         |  splitIds(mcid) as mcid,
         |  splitIds(mcid_tm) as mcid_tm,
         |  splitIds(mcid_ltm) as mcid_ltm,
         |  splitIds(pid) as pid,
         |  splitIds(pid_tm) as pid_tm,
         |  splitIds(pid_ltm) as pid_ltm,
         |  splitIds(isid) as isid,
         |  splitIds(isid_tm) as isid_tm,
         |  splitIds(isid_ltm) as isid_ltm,
         |  duid,
         |  splitIds(snid) as snid,
         |  splitIds(snid_tm) as snid_tm,
         |  splitIds(snid_ltm) as snid_ltm,
         |  splitIds(orig_ieid) as orig_ieid,
         |  splitIds(orig_ieid_tm) as orig_ieid_tm,
         |  splitIds(orig_ieid_ltm) as orig_ieid_ltm,
         |  splitIds(oiid) as oiid,
         |  splitIds(oiid_tm) as oiid_tm,
         |  splitIds(oiid_ltm) as oiid_ltm
         |from $androidTable as android_mapping
         |left join $androidDuidMapping as duid_mapping
         |on android_mapping.device = duid_mapping.device
         |  where isNorm(android_mapping.device)
      """.stripMargin)
      .filter(size(col("ieid")) <= one2ManyThreshold)
      .filter(size(col("mcid")) <= one2ManyThreshold)
      .filter(size(col("isid")) <= one2ManyThreshold)
      .filter(size(col("snid")) <= one2ManyThreshold)


    persistDeviceMapping(day, android, androidDF)

    val iosDuidMapping = duidMapping(ios)
    persistDeviceMapping(day, ios, sql(
      s"""
         |select
         |  ios_mapping.device,
         |  null as ieid,
         |  null as ieid_14,
         |  null as ieid_15,
         |  null as ieid_tm,
         |  null as ieid_ltm,
         |  null as ieid_14_tm,
         |  null as ieid_14_ltm,
         |  null as ieid_15_tm,
         |  null as ieid_15_ltm,
         |  splitIds(ifid) as ifid,
         |  splitIds(ifid_tm) as ifid_tm,
         |  splitIds(ifid_ltm) as ifid_ltm,
         |  splitIds(mcid) as mcid,
         |  splitIds(mcid_tm) as mcid_tm,
         |  splitIds(mcid_ltm) as mcid_ltm,
         |  splitIds(pid) as pid,
         |  splitIds(pid_tm) as pid_tm,
         |  splitIds(pid_ltm) as pid_ltm,
         |  null as isid,
         |  null as isid_tm,
         |  null as isid_ltm,
         |  duid,
         |  null as snid,
         |  null as snid_tm,
         |  null as snid_ltm,
         |  null as orig_ieid,
         |  null as orig_ieid_tm,
         |  null as orig_ieid_ltm,
         |  null as oiid,
         |  null as oiid_tm,
         |  null as oiid_ltm
         |from $iosTable as ios_mapping
         |left join $iosDuidMapping as duid_mapping
         |on ios_mapping.device = duid_mapping.device
         |  where isNorm(ios_mapping.device)
      """.stripMargin)
      .filter(size(col("mcid")) <= one2ManyThreshold))
  }
}
