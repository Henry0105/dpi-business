package com.mob.dataengine.engine.core.profile.export.bt

import java.io.File

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
 * @author juntao zhang
 */
class ProfileBatchBackTrackerTest extends FunSuite with LocalSparkSession {
  import spark.implicits._

  test("test ProfileBatchBackTracker") {
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("create database rp_mobdi_app")
    spark.sql("create database rp_dataengine")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val singleProfileTrackInfoSql = FileUtils.getSqlScript(
      s"conf/sql_scripts/rp_tables_create/rp_dataengine/profile/single_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_SINGLE_PROFILE_TRACK_INFO
    )
    createTable(singleProfileTrackInfoSql)
    val onlineProfileDaySql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_mobdi_app/timewindow_online_profile_day.sql",
      tableName = PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY)
    createTable(onlineProfileDaySql)

    val profileHistoryIndexSql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_mobdi_app/timewindow_online_profile_day.sql",
      tableName = PropUtils.HIVE_TABLE_PROFILE_HISTORY_INDEX)
    createTable(profileHistoryIndexSql)

    val profileDayDF = Seq(
      // 第1个设备
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7501", Map("1_1000" -> "11", "2_1000" -> "2"), "20190501"),
      // 第2个设备
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", Map("1_1000" -> "21", "2_1000" -> "12"), "20190501"),
      // 第3个设备
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7503", Map("1_1000" -> "31"), "20190501"),
      // 第4个设备
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7504", Map("1_1000" -> "31"), "20190502"),
      // 第5个设备
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7505", Map("1_1000" -> "31"), "20190503")
    ).toDF("device", "profile", "day")

    val profileDayTable = PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY

    insertDF2Table(profileDayDF, profileDayTable, Some("day"), false)

    spark.table(profileDayTable).show(false)

    Seq(
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7501", "part-00000", 0L, "20190501", profileDayTable),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", "part-00000", 1L, "20190501", profileDayTable),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7503", "part-00000", 2L, "20190501", profileDayTable),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7504", "part-00000", 1L, "20190502", profileDayTable),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7505", "part-00000", 1L, "20190502", profileDayTable)
    ).toDF("device", "file_name", "row_number", "day", "table")
      .createOrReplaceTempView("tmp_index")

    val indexDF = spark.sql(
      s"""
         |select device, map(day, named_struct('file_name', file_name, 'row_number', row_number)) as feature_index, table,
         |  '1100' as version
         |from tmp_index
       """.stripMargin).coalesce(1)

    insertDF2Table(indexDF, PropUtils.HIVE_TABLE_PROFILE_HISTORY_INDEX, Some("table, version"), false)

    spark.table(PropUtils.HIVE_TABLE_PROFILE_HISTORY_INDEX).show(false)

    val inputDF = Seq(
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7501", "20190501"),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7501", "20190502"),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7501", "20190505"),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", "20190502"),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7505", "20190502")
    ).toDF("device", "day")

//    val tracker = ProfileBatchBackTracker(spark, inputDF, "20190501",
//      Seq("1_1000", "2_1000", "3_1000", "4_1000", "5_1000", "6_1000", "7_1000", "8_1000"), outputUUID = "test1")
//    tracker.submit()
//
//    spark.table(PropUtils.HIVE_TABLE_SINGLE_PROFILE_TRACK_INFO).filter("uuid='test1'").show(false)
  }
}
