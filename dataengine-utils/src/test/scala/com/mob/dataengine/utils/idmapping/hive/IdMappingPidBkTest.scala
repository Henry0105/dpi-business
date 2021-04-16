package com.mob.dataengine.utils.idmapping.hive

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.mob.dataengine.commons.utils.PropUtils._
import com.mob.dataengine.utils.FileUtils
import com.mob.dataengine.utils.idmapping.{IdMappingPidBk, PidBkParams, hive}
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

case class pidInfo(abnormalFlag: Int, devices: Seq[DeviceInfo])

//case class DeviceInfo(device: String, time: String, cnt: Int, profileFlag: Int)

class IdMappingpidBkTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
    spark.conf.set("spark.sql.shuffle.partitions", 10)
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database dm_mobdi_mapping")

    val pidDeviceBkIncrSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/dm_mobdi_master/" +
      s"pid_mapping_history_trace_di.sql",
      tableName = HIVE_PID_DEVICE_TRACK_DF)
    createTable(pidDeviceBkIncrSql)

    val pidBkMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/" +
      s"dm_pid_mapping_bk.sql",
      tableName = HIVE_TABLE_DM_PID_MAPPING_BK)
    createTable(pidBkMappingSql)

    prepareWarehouse()
  }

  def prepareWarehouse(): Unit = {
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict;")

    val schema = spark.table(HIVE_PID_DEVICE_TRACK_DF).schema.fieldNames
    val pidDeviceBkFullDF = Seq(
      ("13322567391",
        Map(
          "20200805" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200805"), 110, 1),
              DeviceInfo("db", String2TimeStamp("20200805"), 100, 1))),
          "20200806" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200806"), 100, 1),
              DeviceInfo("dc", String2TimeStamp("20200806"), 120, 1))),
          "20200807" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200807"), 100, 1),
              DeviceInfo("dc", String2TimeStamp("20200807"), 150, 1))),
          "20200808" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200808"), 100, 1),
              DeviceInfo("dc", String2TimeStamp("20200808"), 150, 1))),
          "20200809" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200809"), 100, 1),
              DeviceInfo("db", String2TimeStamp("20200809"), 150, 1))),
          "20200810" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200810"), 100, 1),
              DeviceInfo("db", String2TimeStamp("20200810"), 100, 1),
              DeviceInfo("dc", String2TimeStamp("20200810"), 100, 1))),
        ), "20200810", "20200810"),
      ("13322567392",
        Map(
          "20200805" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200805"), 120, 1),
              DeviceInfo("dd", String2TimeStamp("20200805"), 50, 1))),
          "20200806" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200806"), 100, 1),
              DeviceInfo("dc", String2TimeStamp("20200806"), 100, 1))),
        ), "20200806", "20200810")
    ).toDF(schema: _*)

    val pidDeviceBkIncrDF = Seq(
      ("13322567392",
        Map(
          "20200805" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200805"), 120, 1),
              DeviceInfo("dd", String2TimeStamp("20200805"), 50, 1))),
          "20200806" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200806"), 100, 1),
              DeviceInfo("dc", String2TimeStamp("20200806"), 100, 1))),
          "20200811" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200811"), 100, 1),
              DeviceInfo("dc", String2TimeStamp("20200811"), 110, 1)))
        ), "20200811", "20200811"),
      ("13322567393",
        Map(
          "20200811" -> pidInfo(0,
            Seq(
              DeviceInfo("da", String2TimeStamp("20200811"), 100, 0),
              DeviceInfo("dc", String2TimeStamp("20200811"), 100, 0),
              DeviceInfo("de", String2TimeStamp("20200811"), 100, 0))),
        ), "20200811", "20200811"),
      ("13322567394", Map.empty[String, pidInfo], "20200811", "20200811"),
      ("13322567395", null, "20200811", "20200811"),
      ("13322567396",
        Map(
          "20200811" -> pidInfo(0,
            Seq(
              DeviceInfo("dx", String2TimeStamp("20200811"), 190, 0),
              DeviceInfo("da", String2TimeStamp("20200811"), 140, 1),
              DeviceInfo("db", String2TimeStamp("20200811"), 140, 1),
              DeviceInfo("dc", String2TimeStamp("20200811"), 120, 1),
              DeviceInfo("dd", String2TimeStamp("20200811"), 110, 1))),
        ), "20200811", "20200811")
    ).toDF(schema: _*)

    insertDF2Table(pidDeviceBkFullDF, HIVE_PID_DEVICE_TRACK_DF, Some("day"), excludePartCols = false)
    insertDF2Table(pidDeviceBkIncrDF, HIVE_PID_DEVICE_TRACK_DF, Some("day"), excludePartCols = false)
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
  }

  private def String2TimeStamp(date: String): String = {
    val time = date + " 00:00:00"
    val fmt = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")
    val parse = LocalDateTime.parse(time, fmt)
    LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli().toString;
  }

  test("test pid => device mappping_incr") {
    println("**********  源数据  **********")
    spark.table(HIVE_PID_DEVICE_TRACK_DF).show(false)

    val fullDay = "20200810"
    val incrDay = "20200811"
    IdMappingPidBk(spark, PidBkParams(fullDay, full = true)).run()
    IdMappingPidBk(spark, PidBkParams(incrDay, full = false)).run()

    println(s"**********  结果$fullDay  **********")
    val fullDF = spark.table(HIVE_TABLE_DM_PID_MAPPING_BK).where($"day" === fullDay)
    fullDF.show(false)
    println(s"**********  结果$incrDay  **********")
    val incrDF = spark.table(HIVE_TABLE_DM_PID_MAPPING_BK).where($"day" === incrDay)
    incrDF.show(false)

    assertResult(2)(fullDF.count())

    assertResult(4)(incrDF.count())
    assertResult(3)(incrDF.where("update_time = 20200811").count())
    assertResult(4)(incrDF.where("pid = 13322567391")
      .collect()(0).getAs[Seq[String]]("bk_tm").length)
    assertResult("de")(incrDF.where("pid = 13322567393")
      .collect()(0).getAs[Seq[String]]("device").last)
  }

}
