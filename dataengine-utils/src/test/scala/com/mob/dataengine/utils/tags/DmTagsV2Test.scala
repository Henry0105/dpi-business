package com.mob.dataengine.utils.tags

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite


class DmTagsV2Test extends FunSuite with LocalSparkSession {
  import spark.implicits._

  val d1 = "d100829fccff588f101380954ad99aca85cdde39"
  val d2 = "d2012ea66d1386e64c84cd81bf6f0076caf2d199"
  val d3 = "d3016d1b4a66d20f39788821882967e9e0b3ee82"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")

    spark.sql("create database dm_dataengine_test")
    spark.sql("create database rp_mobdi_app")

    val dmTagsV2Sql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_test/dm_tags_info_v2.sql",
      tableName = PropUtils.HIVE_TABLE_DM_TAGS_INFO_V2)
    createTable(dmTagsV2Sql)

    val profileDaySql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_mobdi_app/timewindow_online_profile_day.sql",
      tableName = PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY)
    createTable(profileDaySql)

    val profileDayV2Sql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_mobdi_app/timewindow_online_profile_day.sql",
      tableName = PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V2)
    createTable(profileDayV2Sql)

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    insertDF2Table(spark.sql(
      s"""
         |select stack(2,
         |  '$d1', map('p1', array('20200301', 'v1')),
         |  '$d2', map('p2', array('20200231', 'v2'))
         |) as (device, profile)
       """.stripMargin), PropUtils.HIVE_TABLE_DM_TAGS_INFO_V2, Some("day='20200301'"))

    insertDF2Table(spark.sql(
      s"""
         |select stack(3,
         |  '$d1', map('p2', 'v1'), '20200302',
         |  '$d2', map('p2', 'v1'), '20200301',
         |  '$d3', map('p2', 'v2'), '20200301'
         |) as (device, profile, day)
       """.stripMargin), PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY, Some("day"), false)

    insertDF2Table(spark.sql(
      s"""
         |select stack(2,
         |  '$d1', map('p3', 'v3'), '20200302',
         |  '$d2', map('p3', 'v4'), '20200302'
         |) as (device, profile, day)
       """.stripMargin), PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V2, Some("day"), false)
  }

  test("can merge") {
    val t = DmTagsV2Generator(spark)
    val df = t.buildDaysProfile("20200301", "20200302")

    df.show(false)

    t.mergeIntoFull(df, "20200302")

    val res = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_V2}
         |where day = '20200302'
       """.stripMargin).cache()

    val m = res.map{ r => (r.getString(0), r.getAs[Map[String, Seq[String]]]("profile"))}
        .collect().toMap


    assertResult(Seq("20200301", "v1"))(m(d1)("p1"))
    assertResult(Seq("20200302", "v1"))(m(d1)("p2"))
    assertResult(Seq("20200302", "v3"))(m(d1)("p3"))
    assertResult(Seq("20200301", "v1"))(m(d2)("p2"))
    assertResult(Seq("20200301", "v2"))(m(d3)("p2"))
  }
}
