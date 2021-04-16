package com.mob.dataengine.utils.crowd

import com.mob.dataengine.commons.helper.BitDate
import com.mob.dataengine.commons.utils.PropUtils._
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.{DataFrame, LocalSparkSession}
import org.scalatest.FunSuite

class CrowdAppTimeGeneratorTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
    spark.conf.set("spark.sql.shuffle.partitions", 10)
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_master CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql("create database dm_dataengine_tags")
    spark.sql("create database dm_mobdi_master")
    spark.sql("create database dm_sdk_mapping")

    /* sourceTable */
    val masterReservedNewSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_mobdi_master/master_reserved_new.sql",
      tableName = HIVE_TABLE_MASTER_RESERVED_NEW)
    createTable(masterReservedNewSql)

    val deviceActiveApplistSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_mobdi_master/device_active_applist.sql",
      tableName = HIVE_TABLE_DEVICE_ACTIVE_APPLIST)
    createTable(deviceActiveApplistSql)

    val pkgNameMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_sdk_mapping/pkg_name_mapping.sql",
      tableName = HIVE_TABLE_DM_PKG_NAME_MAPPING)
    createTable(pkgNameMappingSql)

    /* targetTable */
    val appTableSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      "dm_dataengine_tags/device_app_time_status_full.sql",
      tableName = HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll)
    createTable(appTableSql)

    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_master CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
  }

  def prepareWarehouse(): Unit = {
    spark.sql("set hive.exec.dynamici.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    /* dm_mobdi_master.master_reserved_new */
    val fieldNames1 = spark.table(HIVE_TABLE_MASTER_RESERVED_NEW).schema.fieldNames
    Seq(
      ("d1", "pkg-1", 0, 0, 0, 0, "20201025"),
      ("d1", "pkg-2", 1, 0, 0, 0, "20200721"),
      ("d1", "pkg-3", 1, 0, 0, 0, "20200513"),
      ("d1", "pkg-3", 1, 0, 0, 0, "20200609"),
      ("d1", "pkg-4", 2, 0, 0, 0, "20200219"),
      ("d1", "pkg-1", 2, 0, 0, 0, "20201030"),
      ("d1", "pkg-1", 2, 0, 0, 0, "20201031"),
      ("d5", "pkg-10", 1, 0, 0, 0, "20201030"),
      ("d7", "pkg-99", 0, 0, 0, 0, "20201030"),
      ("d7", "pkg-99", 0, 0, 0, 0, "20201031"),
      ("d7", "pkg-99", 0, 0, 0, 0, "20201101")
    ).toDF(fieldNames1: _*).createOrReplaceTempView("prepare_1")

    spark.sql(
      s"""
         |insert overwrite table $HIVE_TABLE_MASTER_RESERVED_NEW
         |select ${fieldNames1.mkString(", ")}
         |from prepare_1
       """.stripMargin)

    /* dm_mobdi_master.device_active_applist */
    val fieldNames2 = spark.table(HIVE_TABLE_DEVICE_ACTIVE_APPLIST).schema.fieldNames
    Seq(
      ("d1", 1, "pkg-1", "", "", "20191031"),
      ("d1", 1, "pkg-1", "", "", "20201026"),
      ("d1", 1, "pkg-2", "", "", "20200413"),
      ("d1", 1, "pkg-3", "", "", "20200922"),
      ("d1", 1, "pkg-3", "", "", "20200824"),
      ("d1", 1, "pkg-4", "", "", "20200708"),
      ("d1", 1, "pkg-1", "", "", "20201030"),
      ("d1", 1, "pkg-1", "", "", "20201031"),
      ("d1", 1, "pkg-1", "", "", "20201101"),
      ("d6", 1, "pkg-12", "", "", "20201030"),
      ("d7", 1, "pkg-99", "", "", "20201101"),
      ("d9", 1, "pkg-1", "", "", "20191031")
    ).toDF(fieldNames2: _*).createOrReplaceTempView("prepare_2")
    spark.sql(
      s"""
         |insert overwrite table $HIVE_TABLE_DEVICE_ACTIVE_APPLIST
         |select ${fieldNames2.mkString(", ")}
         |from prepare_2
       """.stripMargin)

    /* dm_sdk_mapping.pkg_name_mapping */
    val fieldNames3 = spark.table(HIVE_TABLE_DM_PKG_NAME_MAPPING).schema.fieldNames
    Seq(
      ("pkg-1", "包名-01", 0, "20200101"),
      ("pkg-2", "包名-02", 0, "20200101"),
      ("pkg-3", "包名-03", 0, "20200101")
    ).toDF(fieldNames3: _*).createOrReplaceTempView("prepare_3")

    spark.sql(
      s"""
         |insert overwrite table $HIVE_TABLE_DM_PKG_NAME_MAPPING
         |select ${fieldNames3.mkString(", ")}
         |from prepare_3
       """.stripMargin)
  }

  def genAppTimeTable(day: String, full: Boolean): Unit = {
    val p = CrowdAppTimeParam(day, full)
    CrowdAppTime(spark, p).run()
  }

  def getAppTimeTableResult(day: String): DataFrame = {
    spark.sql(
      s"""
         |SELECT *
         |FROM $HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll
         |WHERE day = '$day'
         |ORDER BY device, pkg
         |""".stripMargin)
  }

  test("测试full表生成") {
    genAppTimeTable("20201029", full = true)
    println("=========================== 结果表 2020年10月29日 ===========================")
    val df = getAppTimeTableResult("20201029")
    df.show(false)

    assertResult(5)(df.where("upper_time <= '20201029'").count())
  }

  test("测试增量更新") {
    genAppTimeTable("20201029", full = true)
    genAppTimeTable("20201030", full = false)
    val df29 = getAppTimeTableResult("20201029")
    val df30 = getAppTimeTableResult("20201030")
    println("=========================== 结果表 2020年10月29日 ===========================")
    df29.show(false)
    println("=========================== 结果表 2020年10月30日 ===========================")
    df30.show(false)

    assertResult(4)(df30.where("upper_time = '20201030'").count())
  }

  test("测试月首末旧分区替换") {
    genAppTimeTable("20201030", full = true)
    genAppTimeTable("20201031", full = false)
    genAppTimeTable("20201101", full = false)
    val df30 = getAppTimeTableResult("20201030")
    val df31 = getAppTimeTableResult("20201031")
    val df01 = getAppTimeTableResult("20201101")
    println("=========================== 结果表 2020年10月30日 ===========================")
    df30.show(false)
    println("=========================== 结果表 2020年10月31日 ===========================")
    df31.show(false)
    println("=========================== 结果表 2020年11月01日 ===========================")
    df01.show(false)

    assertResult(1)(df31.where($"device" === "d9").count())
    assertResult(0)(df01.where($"device" === "d9").count())
    assertResult(BitDate.calDayInt(1))(
      df01.where($"device" === "d1" && $"pkg" === "pkg-1").select("active")
        .collect().head.getAs[Seq[Int]](0).head
    )
    assertResult(BitDate.calDayInt(1))(
      df01.where($"device" === "d7" && $"pkg" === "pkg-99").select("installed")
        .collect().head.getAs[Seq[Int]](0).head
    )
  }

}
