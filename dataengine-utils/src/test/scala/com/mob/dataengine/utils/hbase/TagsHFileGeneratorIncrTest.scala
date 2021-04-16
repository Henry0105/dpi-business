package com.mob.dataengine.utils.hbase

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class TagsHFileGeneratorIncrTest extends FunSuite with LocalSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_tags")
    val tagsInfoSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_tags/dm_profile_tags_info_full.sql",
      tableName = PropUtils.HIVE_TABLE_PROFILE_TAGS_INFO_FULL)
    createTable(tagsInfoSql)

    val tagsInfoDF = spark.sql(
      s"""
         |select "5adde9f7e36aa79106b6f6cf35cbc7ff2068b411" device,
         |  map('3_1000', array('97', '20200630'),
         |      '64_1000', array('1',  '20200630')) tags,
         |  map('3_1000', array("0.7", '20200601'),
         |      '4_1000', array("0.1", '20200630')) confidence
         |union all
         |select "5addeb833013fea5739c9987d3e4b50457dd0653" device,
         |  map('3_1000', array('97', '20200630'),
         |      '4_1000', array('1',  '20200630'),
         |      '1034_1000', array('1,2', '20200615'),
         |      '7_1000', array('1', '20200601')) tags,
         |  map('4_1000', array("0.1", '20200630')) confidence
         |union all
         |select "5addecbcb09afa7c89d4a416d49c33bcbd82a6cc" device,
         |  map("3_1000", array('unknown', '20200601'),
         |      "4_1000", array('unknown', '20200601'),
         |      "5436_1000", array('1,2', '20200630'),
         |      "7_1000", array('1',  '20200601')) tags,
         |  map( "4_1000", array("0.1", '20200615')) confidence
       """.stripMargin)
    insertDF2Table(tagsInfoDF, PropUtils.HIVE_TABLE_PROFILE_TAGS_INFO_FULL, Some("day=20200630"))
  }

  test("测试 生成hfile的源数据生成的结果") {
    val day = "20200630"
    val df = AndroidTagsHFileGenerator(spark, day).transformData(full = false)
    println("结果是")
    df.show(false)
  }

}
