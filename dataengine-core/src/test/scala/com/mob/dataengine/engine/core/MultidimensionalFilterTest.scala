package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.crowd.{CrowdFilter, MultidimensionalFilter}
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class MultidimensionalFilterTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_mobdi_app")

    // 创建存储结果表
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_dataengine")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_tags")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_dataengine_test")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_test")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_mapping")

    val codeMappingSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_code_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
    println(s"now print code mapping info $codeMappingSql")
    createTable(codeMappingSql)

    val dataHubSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/profile/single_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_HUB)
    println(s"now print data hub info $dataHubSql")
    createTable(dataHubSql)

    val dataOPTCacheNewSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      "rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val dataOPTCacheNewSql1 = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      "rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheNewSql1)

    prepareCodeMapping()
    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    //    stop()
  }

  def prepareWarehouse(): Unit = {
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='1|4',
         |  uuid='uuid123')
         |select 'd1' as data
         |union all
         |select 'd2' as data
       """.stripMargin)

    Seq(
      ("d1",
        Map("3_1000" -> "8", "4_1000" -> "1", "7_1000" -> "1",
          "1036_1000" -> "cn", "1034_1000" -> "2,3", "2_1001" -> "4"),
        Map("3_1000" -> "8"), "", "", "20190408"),
      ("d2",
        Map("3_1000" -> "9", "4_1000" -> "0", "7_1000" -> "1",
          "1036_1000" -> "cn", "2_1001" -> "3"),
        Map("3_1000" -> "9"), "", "", "20190408"),
      ("d3",
        Map("3_1000" -> "8", "4_1000" -> "1",
          "1034_1000" -> "1,2", "7_1000" -> "1",
          "1036_1000" -> "cn", "2_1001" -> "-1"),
        Map("4_1000" -> "1"), "", "", "20190408"),
      ("d4",
        Map("1_1000" -> "5",
          "2_1000" -> "5",
          "3_1000" -> "5",
          "4_1000" -> "5",
          "5_1000" -> "5"),
        Map("4_1000" -> "1"), "", "", "20190408")
    ).toDF(
      "device", "tags", "confidence", "update_time", "grouped_tags", "day"
    ).write.partitionBy("day")
      .saveAsTable(PropUtils.HIVE_TABLE_DM_TAGS_INFO)

    spark.sql(
      s"""
         |CREATE VIEW ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_VIEW} AS
         |SELECT device,tags,confidence,update_time,grouped_tags
         |FROM ${PropUtils.HIVE_TABLE_DM_TAGS_INFO}
         |""".stripMargin
    )



  }

  test("multidimensional filter has hub test") {

        Seq(
          (Map("seed" -> Array("d3", "20100622"), "4" -> Array("d1", "20100622")), "uuid345"),
          (Map("seed" -> Array("d4", "20200623"), "4" -> Array("d2", "20200623")), "uuid345")
        ).toDF(
          "feature", "uuid"
        ).write
          .insertInto(PropUtils.HIVE_TABLE_DATA_HUB)




    val uuidOut = "uuid234"
    val myJSON =
      s"""{
    "jobId":"profile_cal_1",
    "jobName":"multidimensional_filter",
    "day":"20190110",
    "rpcHost": null,
    "userId": "145",
     "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"uuid",
                    "uuid":"uuid345",
                    "value" : "",
                    "numContrast":{
                        "4_1000":"<=7"
                    }
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid":"uuid234",
                "limit":2000,
                "idTypes": [4],
                "encrypt": {"encryptType": 0}
            }
        }
    ]
  }"""

    MultidimensionalFilter.main(Array(myJSON))
//    val param = MultidimensionalFilter.jobContext.params.head
//    println(param)
    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}

      """.stripMargin).show(false)
  }

  test("multidimensional filter 不传种子包 test") {
    val uuidOut = "uuid234"
    val myJSON =
      s"""{
    "jobId":"profile_cal_1",
    "jobName":"multidimensional_filter",
    "day":"20190110",
    "rpcHost": null,
    "userId": "145",
     "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType": "empty",
                    "uuid": null,
                    "value": null
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid": "$uuidOut",
                "limit":2000,
            }
        }
    ]
  }"""

    MultidimensionalFilter.main(Array(myJSON))
//    val param = MultidimensionalFilter.jobContext.params.head
//    println(param)
    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin)
    df.show(false)

    assertResult(3)(df.count())
  }

  test("multidimensional filter sql test") {
    Seq(
      (Map("seed" -> Array("d3", "20100622"), "4" -> Array("d1", "20100622")), "uuid345"),
      (Map("seed" -> Array("d4", "20200623"), "4" -> Array("d2", "20200623")), "uuid345")
    ).toDF(
      "feature", "uuid"
    ).write
      .insertInto(PropUtils.HIVE_TABLE_DATA_HUB)

    val uuidOut = "uuid123"
    val myJSON =
      s"""{
    "jobId":"profile_cal_1",
    "jobName":"multidimensional_filter",
    "day":"20190110",
    "rpcHost": null,
    "userId": "145",
     "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"sql",
                    "uuid": "select @phone@ as phone,@d1@ as device",
                    "idx" : 2,
                    "sep" : ",",
                    "numContrast":{
                        "4_1000":"<=7"
                    }
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid": "$uuidOut",
                "limit":2000,
                "idTypes": [4],
                "encrypt": {"encryptType": 0}
            }
        }
    ]
  }"""
    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).show(false)
    MultidimensionalFilter.main(Array(myJSON))
//    val param = MultidimensionalFilter.jobContext.params.head
//    println(param)
    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin)
    df.show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

  }



  test("multidimensionalFilter numContrast test") {
    val uuidOut = "uuid234"
    val myJSON =
      s"""{
    "jobId":"profile_cal_1",
    "jobName":"multidimensional_filter",
    "day":"20190110",
    "rpcHost": null,
    "userId": "145",
    "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType": "empty",
                    "uuid": null,
                    "value": null,
                    "numContrast":{
                        "4_1000":"<=7"
                    }
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid": "$uuidOut",
                "limit":2000
            }
        }
    ]
  }"""

    MultidimensionalFilter.main(Array(myJSON))

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)
  }

}
