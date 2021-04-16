package com.mob.dataengine.utils.pidXid

import org.aiav.xidlabelsoftsdk.service.generate.XidlabelGenerateService
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mob.dataengine.commons.utils.PropUtils

object PidXidLabel {
  @transient private[this] val logger = Logger.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val day = args(0)
    val isFull = args(1)
    val yesterday = args(2)
    val spark = SparkSession.builder
      .enableHiveSupport()
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()
    val acc = spark.sparkContext.longAccumulator("exception")

    val readTable = PropUtils.HIVE_TABLE_DM_PID_MAPPING
    val sql = if (isFull.toBoolean) {
      s"select pid, pid_decrypt(pid) as phone_md5 from $readTable where day=$day"
    } else {
      s"""
         |select pid, pid_decrypt(pid) as phone_md5 from $readTable
         |where day=$day and update_time>=unix_timestamp($day,'yyyyMMdd')
        """.stripMargin
    }
    // 注册udf
    spark.sql("create temporary function pid_decrypt as 'com.mob.udf.PidDecrypt'")
    val pidDF: DataFrame = spark.sql(sql).repartition(1000)

    import spark.implicits._
    val pidXidDF: DataFrame = pidDF.mapPartitions(iterator => {
      val generateService = new XidlabelGenerateService()
      iterator.flatMap(row => {
        val pid: String = row.getAs[String]("pid")
        val phoneMd5: String = row.getAs[String]("phone_md5")
        try {
          val xidLabel: String = generateService.generateXidlabelById("5", phoneMd5)
          Some((pid, xidLabel))
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            acc.add(1L)
            None
        }
      })
    }).toDF("pid", "xid_label")
    pidXidDF.createOrReplaceTempView("tmp_table")

    val writeTable = PropUtils.HIVE_TABLE_DM_DATAENGINE_PID_XIDLABEL
    val write_sql = if (isFull.toBoolean) {
      s"""
         |insert overwrite table $writeTable partition(day=$day)
         |select pid, xid_label from tmp_table
        """.stripMargin
    } else {
      s"""
         |insert overwrite table $writeTable partition(day=$day)
         |select pid, xid_label from tmp_table
         |union all
         |select pid, xid_label from $writeTable where day=$yesterday
        """.stripMargin
    }
    spark.sql(write_sql)
    logger.info("write_sql: " + write_sql)
    logger.info("exception count: " + acc.sum)
  }
}