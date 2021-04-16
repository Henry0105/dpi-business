package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.json4s.DefaultFormats
import org.scalatest.FunSuite

class DataCleaningTest extends FunSuite with LocalSparkSession with UDFCollections with Serializable {
  implicit val formats: DefaultFormats.type = DefaultFormats

  val jobCommon: JobCommon = new JobCommon("jobId", "jobName", "rpcHost", 0, "day")

  import spark.implicits._
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("create database rp_dataengine")

    val dataOPTCacheNewSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val dataOPTCacheSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)
  }
  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
  }

  def prepareWarehouse(): Unit = {
    val dataOptDF = Seq(
      "phone,name,age",
      "16602113341,jianDC:1,22",
      "16602113342,jianDD2,23",
      "16602113343,jianDD3,24",
      "16602113344,jianDD4,25",
      "16602113345,jianDD5,26"
    ).toDF("data")

    insertDF2Table(dataOptDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20190101', biz='3|1', uuid='jiandd_123'"))
  }
  test("dataCleaning") {
    prepareWarehouse()
    val json =
      s"""
         |{
         |  "rpc_port": 0,
         |  "rpc_host": "127.0.0.1",
         |  "jobId":"33dd2ac7cb5547a49650e1cb968d0dc3",
         |  "jobName":"data_cleaning",
         |  "day":"20190101",
         |  "params":[
         |    {
         |      "inputs":[
         |        {
         |          "idType":0,
         |          "uuid":"jiandd_123",
         |          "sep":",",
         |          "idx":2,
         |          "header":1,
         |          "headers":["in_phone","in_name","in_age"]
         |        }
         |      ],
         |      "output":{
         |        "limit":2000,
         |        "module":"demo",
         |        "uuid":"jiandd_234",
         |        "cleanImei":0,
         |        "keepSeed":0,
         |        "regexpClean":"[A-C]",
         |        "transform":{
         |          "delete":[" "],
         |          "fillImei":["1"],
         |          "lower":["1"]
         |        }
         |      }
         |    }
         |  ]
         |}
       """.stripMargin
    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = 'jiandd_123'
        """.stripMargin).show(false)
    DataCleaning.main(Array(json))
    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'jiandd_234'
        """.stripMargin)
    df.show(false)
    assert(df.count()==4)

    println(jobCommon)

  }

}
