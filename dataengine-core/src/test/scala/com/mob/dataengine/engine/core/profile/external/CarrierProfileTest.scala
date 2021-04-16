package com.mob.dataengine.engine.core.profile.external

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.traits.Logging
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.profilecal.external._
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class CarrierProfileTest extends FunSuite with LocalSparkSession with Logging {
  val profileIds: Seq[String] = Seq("2_1001", "3_1000", "4466_1000", "1034_1000", "7_1000", "4_1000")
  val jobCommon: JobCommon = new JobCommon("jobId", "jobName",
    "rpcHost", 0, "day")

  val businessId = 9L
  val productId = "stats"
  val userId = 319220373160185856L
  val carrierJson: String = FileUtils.getJson("unittest/profile_cal/external/carrier.json")
  val getuiJson: String = FileUtils.getJson("unittest/profile_cal/external/getui.json")
  val jiguangJson: String = FileUtils.getJson("unittest/profile_cal/external/jiguang.json")

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._

    // 创建存储结果表
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_dataengine_test")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_test")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_mapping")

    val dataHubSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/profile/single_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_HUB)
    println(s"now print data hub info $dataHubSql")
    createTable(dataHubSql)

    val codeMappingSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_code_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
    println(s"now print code mapping info $codeMappingSql")
    createTable(codeMappingSql)

    val extProfileSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_test/dm_tags_info_v2.sql",
      tableName = PropUtils.HIVE_TABLE_EXT_LABEL_RELATION_FULL)
    println(s"now print ext profile info $extProfileSql")
    createTable(extProfileSql)
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val dataHubDF = sql(
      s"""
         |select map('seed', array('18699476687')) feature, 'cp_in_001' uuid
         |union all
         |select map('seed', array('e0aaad81bce9b5a3748b761103d399d6')) feature, 'gp_in_001' uuid
         |union all
         |select map('seed', array('865131041548071')) feature, 'jp_in_001' uuid
       """.stripMargin)
    insertDF2Table(dataHubDF, PropUtils.HIVE_TABLE_DATA_HUB, Some("uuid"), false)

    prepareCodeMapping()
  }


  override def afterAll(): Unit = {
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
  }

  test("运营商画像") {
    import spark.implicits._
    CarrierProfile.main(Array(carrierJson))

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = 'cp_out_001'
       """.stripMargin).show(false)
    assert(spark.read.text("hdfs_output_cp_out_001").map(_.getString(0)).head(2).last.startsWith("18699476687"))
    deleteHdfsPath("hdfs_output_cp_out_001")
  }

  test("个推画像") {
    import spark.implicits._
    GeTuiProfile.main(Array(getuiJson))

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = 'gp_out_001'
       """.stripMargin).show(false)
    assert(spark.read.text("hdfs_output_gp_out_001")
      .map(_.getString(0)).head(2).last.startsWith("e0aaad81bce9b5a3748b761103d399d6"))
    deleteHdfsPath("hdfs_output_gp_out_001")
  }

  test("极光画像") {
    import spark.implicits._
    JiGuangProfile.main(Array(jiguangJson))

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = 'jp_out_001'
       """.stripMargin).show(false)
    spark.read.text("hdfs_output_jp_out_001").show(false)

    assert(spark.read.text("hdfs_output_jp_out_001").map(_.getString(0)).head(2).last.startsWith("865131041548071"))
    assert(spark.read.text("hdfs_output_jp_out_001").map(_.getString(0)).head(1).last.startsWith("种子包"))

    deleteHdfsPath("hdfs_output_jp_out_001")
  }
}
