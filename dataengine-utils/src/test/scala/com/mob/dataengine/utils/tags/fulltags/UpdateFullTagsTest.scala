package com.mob.dataengine.utils.tags.fulltags

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class UpdateFullTagsTest extends FunSuite with LocalSparkSession{
  import spark.implicits._
  val FullTagTable: String = PropUtils.HIVE_TABLE_PROFILE_TAGS_INFO_FULL
  val updateTable: String = PropUtils.HIVE_TABLE_PROFILE_ALL_TAGS_INFO_DI

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql("CREATE DATABASE dm_dataengine_tags")
    spark.sql("CREATE DATABASE rp_mobdi_app")



    spark.sql(
      s"""create table $FullTagTable
         |(
         |device string,
         |tags map<string,Array<string>>,
         |confidence map<string,Array<string>>
         |)
         |partitioned by (day string)""".stripMargin)

    spark.sql(
      s"""create table $updateTable
         |(
         |device string,
         |tags map<string,Array<string>>,
         |confidence map<string,Array<string>>
         |)
         |partitioned by (day string)""".stripMargin)

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    insertDF2Table(Seq(
      ("device1",
        Map("tag1" -> Array("value1", "20200601", "20200630"), "tag3" -> Array("value1", "20200601", "20200630")),
        Map("confidence1" -> Array("confidence1", "20200702", "20200630")))
    ).toDF("device", "tags", "confidence"), FullTagTable, Some("day=20200702"))

    insertDF2Table(Seq(
      ("device1",
        Map("tag2" -> Array("value2", "20200602"),
        "tag1" -> Array("value11", "20200602") ),
        Map("confidence1" -> Array("confidence11", "20200703"),
          "confidence2" -> Array("confidence22", "20200703"))),
      ("device2",
        Map("tag2" -> Array("value2", "20200602"),
          "tag1" -> Array("value11", "20200602") ),
        Map("confidence1" -> Array("confidence11", "20200703"),
          "confidence2" -> Array("confidence22", "20200703")))
    ).toDF("device", "tags", "confidence"), updateTable, Some("day=20200702"))
  }

  test("update_table") {

    val par = s"""--day=20200704"""

    UpdateFullTags.main(Array(par))


    spark.sql(
      s"""
         |select * from $FullTagTable
         |""".stripMargin).show(false)

    spark.sql(
      s"""
         |select * from $updateTable
         |""".stripMargin).show(false)






  }




}
