package com.mob.dataengine.utils.idmapping

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object SnsuidDeviceMapping {
  def main(args: Array[String]): Unit = {
    val day = args(0)
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().
      appName(s"snssuid_device_mapping_$day").getOrCreate()

    import spark.implicits._

    // 获取社交平台的社交账号对应的device和数据获取时间
    val snssuidDeviceDF = getSnssuidDevice(spark, PropUtils.HIVE_TABLE_RP_DEVICE_SNS_FULL)

    // 变更数据的格式，拆分device和对应时间的字段
    val resultDF: DataFrame = snssuidDeviceDF.map(row => {
      val snsuidPlat = row.getAs[String]("snsuid_snsplat")
      val deviceDevtm: Seq[String] = row.getAs[Seq[String]]("device_devicetm")
      val update = row.getAs[String]("update")

      val deviceDevTmTuple: (Seq[String], Seq[String]) = getLatestElements(deviceDevtm)

      (snsuidPlat, deviceDevTmTuple._1, deviceDevTmTuple._2, update)
    }).toDF("snsuid_snsplat", "device", "device_tm", "update")

    resultDF.repartition(500, $"device").createTempView("result")

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DM_SNSUID_DEVICE_MAPPING} PARTITION(day=$day)
         |SELECT
         |    snsuid_snsplat,
         |    device,
         |    device_tm,
         |    update
         |FROM
         |    result
       """.stripMargin)
  }

  /**
   *
   * @param arr
   * @return
   */
  def getLatestElements(arr: Seq[String]): (Seq[String], Seq[String]) = {
    if (null == arr || arr.isEmpty) {
      (null, null)
    } else {
      val tmp = arr.map(e => (e.split("\u0001")(0), e.split("\u0001")(1))).sortWith((a, b) => a._2 > b._2)
      (tmp.map(_._1), tmp.map(_._2))
    }
  }

  /**
   * 获取各个社交平台的社交账号对应的device和数据获取时间
   * 获取的数据需要经过去重
   * @param spark
   * @param table
   * @return
   */
  def getSnssuidDevice(spark: SparkSession, table: String): DataFrame = {
    val sql =
      s"""
         |SELECT
         |    snsuid_snsplat,
         |    collect_set(CONCAT_WS('\u0001',device,device_tm)) as devic_devicetm,
         |    max(processtime) as update
         |FROM(
         |    SELECT
         |        CONCAT_WS('_',snsuid,cast (snsplat as STRING)) as snsuid_snsplat,
         |        device,
         |        processtime as device_tm,
         |        processtime
         |    FROM
         |        $table
         |    UNION ALL
         |    SELECT
         |        CONCAT_WS('_',nickname,cast (snsplat as STRING)) as snsuid_snsplat,
         |        device,
         |        processtime as device_tm,
         |        processtime
         |    FROM
         |        $table
         |    WHERE
         |        snsplat = 11
         |        AND nickname is not null
         |        AND length(nickname) > 0
         |)t1
         |GROUP BY
         |    snsuid_snsplat
         |
       """.stripMargin
    spark.sql(
      sql
    ).toDF("snsuid_snsplat", "device_devicetm", "update")
  }
}
