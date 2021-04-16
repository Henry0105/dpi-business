package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class CrowdAppTimeFilterTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // 创建存储结果表
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_dataengine_test")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_dataengine")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_tags")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_test")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_tags")

    val appTableSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      "dm_dataengine_tags/device_app_time_status_full.sql",
      tableName = PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll)
    createTable(appTableSql)

    val dataOPTCacheSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      "rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)

    val dataOPTCacheNewSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      "rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val dataHubSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/profile/single_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_HUB)
    createTable(dataHubSql)

    val codeMappingSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_code_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
    println(s"now print code mapping info $codeMappingSql")
    createTable(codeMappingSql)


    prepareCodeMapping()
    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
  }

  def prepareWarehouse(): Unit = {
    val fieldNames = spark.table(PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll).schema.fieldNames.init
    val filled = Seq.fill(13)(1)
    Seq(
      ("d1", "pkg-1", "包名-1", filled, null, null, null, "20201026"),
      ("d1", "pkg-2", "包名-2", filled, null, null, null, "20201026"),
      ("d1", "pkg-3", "包名-3", filled, null, null, null, "20201026"),
      ("d1", "pkg-4", "包名-4", filled, null, null, null, "20201026"),
      ("d1", "pkg-5", "包名-4", filled, null, null, null, "20201026"),
      ("d2", "pkg-1", "包名-1", filled, null, null, null, "20201026"),
      ("d3", "pkg-1", "包名-1", filled, null, null, null, "20201026"),
      ("d3", "pkg-2", "包名-2", filled, null, null, null, "20201026"),
      ("d4", "pkg-1", "包名-1", filled, null, null, null, "20201026"),
      ("d4", "pkg-2", "包名-2", filled, null, null, null, "20201026"),
      ("d4", "pkg-3", "包名-3", filled, null, null, null, "20201026"),
      ("d5", "PKG-1", "斑马ai", filled, null, null, null, "20201026")
    ).toDF(fieldNames: _*).createOrReplaceTempView("prepare_1")

    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll} partition(day='20201026')
         |select ${fieldNames.mkString(", ")}
         |from prepare_1
       """.stripMargin)

    println("dm_dataengine_tags.device_app_time_status_full show:")
    spark.table(PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll).where("day == '20201026'").show(false)
  }
  test("基本测试") {
    val uuidIn = "uuid123"
    val uuidOut = "uuid234"
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20201026, biz='1|4',
         |  uuid='$uuidIn')
         |select 'd1' as data
         |union all
         |select 'd1' as data
         |union all
         |select 'd2' as data
       """.stripMargin)

    val myJSON =
      s"""{
    "jobId":"test",
    "jobName":"crowd_app_time_filter",
    "day":"20201026",
    "rpcHost": null,
    "userId": "145",
    "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"uuid",
                    "uuid":"$uuidIn",
                    "value" : "$uuidIn",
                    "rules": [{
                             "include": 1,
                             "timeInterval":["20200521","20201015"],
                             "appStatus":0,
                             "appList":["pkg-1","pkg-2","pkg-3"],
                             "numberRestrict":"1",
                             "appType":2
                    }]
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid":"$uuidOut",
                "limit":2000
            }
        }
    ]
  }"""

    CrowdAppTimeFilter.main(Array(myJSON))

    val cacheNewDF = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin)
    cacheNewDF.show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

    assertResult(2)(cacheNewDF.count())
    assertResult(Array("d1"))(cacheNewDF.collect().map(_.getAs[String]("data")).distinct)
  }
  test("测试DateHub") {
    val uuidIn = "uuid123"
    val uuidOut = "uuid234"
    Seq(
      (Map("seed" -> Array("p1,20100621", "20100621"), "4" -> Array("d1", "20100621")), uuidIn),
      (Map("seed" -> Array("p1,20100622", "20100622"), "4" -> Array("d1", "20100622")), uuidIn),
      (Map("seed" -> Array("p2", "20200623"), "4" -> Array("d2", "20200623")), uuidIn)
    ).toDF(
      "feature", "uuid"
    ).write
      .insertInto(PropUtils.HIVE_TABLE_DATA_HUB)

    val myJSON =
      s"""{
    "jobId":"test",
    "jobName":"crowd_app_time_filter",
    "day":"20201026",
    "rpcHost": null,
    "userId": "145",
    "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"uuid",
                    "uuid":"$uuidIn",
                    "value" : "$uuidIn",
                    "timeInterval":["20200521","20201015"],
                    "appStatus":0,
                    "rules": [{
                             "include": 1,
                             "timeInterval":["20200521","20201015"],
                             "appStatus":0,
                             "appList":["pkg-1","pkg-2","pkg-3"],
                             "numberRestrict":"0",
                             "appType":2
                     }]
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid":"$uuidOut",
                "limit":2000
            }
        }
    ]
  }"""

    CrowdAppTimeFilter.main(Array(myJSON))

    val cacheNewDF = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin)
    cacheNewDF.show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

    assertResult(3)(cacheNewDF.count())
    assertResult("d1,d1,d2")(cacheNewDF.collect().map(_.getAs[String]("data")).sorted.mkString(","))
  }
  test("测试 empty") {
    val uuidOut = "uuid234"

    val myJSON =
      s"""{
    "jobId":"test",
    "jobName":"crowd_app_time_filter",
    "day":"20201026",
    "rpcHost": null,
    "userId": "145",
    "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"empty",
                    "uuid": null,
                    "value" : null,
                    "timeInterval":["20200521","20201015"],
                    "appStatus":0,
                    "rules": [{
                             "include": 1,
                             "timeInterval":["20200521","20201015"],
                             "appStatus":0,
                             "appList":["pkg-1","pkg-2","pkg-3"],
                             "numberRestrict":"1,3",
                             "appType":2
                    }]
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid":"$uuidOut",
                "limit":2000
            }
        }
    ]
  }"""

    CrowdAppTimeFilter.main(Array(myJSON))

    val cacheNewDF = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin)

    cacheNewDF.show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

    assertResult(3)(cacheNewDF.count())
    assertResult("d2,d3,d5")(cacheNewDF.collect().map(_.getAs[String]("data")).sorted.mkString(","))

  }
  test("测试 种子包中携带appList") {
    val uuidIn = "uuid123"
    val uuidOut = "uuid234"
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20201026, biz='1|4',
         |  uuid='$uuidIn')
         |select 'd1,pkg-1#pkg-2#pkg-3' as data
         |union all
         |select 'd1,pkg-1#pkg-2,' as data
         |union all
         |select 'd2,pkg-1#pkg-2#pkg-3' as data
       """.stripMargin)

    val myJSON =
      s"""{
    "jobId":"test",
    "jobName":"crowd_app_time_filter",
    "day":"20201026",
    "rpcHost": null,
    "userId": "145",
    "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"uuid",
                    "uuid":"$uuidIn",
                    "value" : "$uuidIn",
                    "sep":",",
                    "idx":1,
                    "timeInterval":["20200521","20210503"],
                    "appStatus":0,
                    "rules": [{
                             "include": 1,
                             "timeInterval":["20200521","20210503"],
                             "appStatus":0,
                             "numberRestrict":"1",
                             "appType":2
                    }],
                    "numberRestrict":"1",
                    "appListIndex":2,
                    "appListSep":"#"
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid":"$uuidOut",
                "limit":2000
            }
        }
    ]
  }"""

    CrowdAppTimeFilter.main(Array(myJSON))

    val cacheNewDF = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin)
    cacheNewDF.show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

    assertResult(2)(cacheNewDF.count())
    assertResult(Array("d1"))(cacheNewDF.collect().map(_.getAs[String]("data")).distinct)
  }

  test("测试 种子包中携带appList且不区分大小写") {
    val uuidIn = "uuid123"
    val uuidOut = "uuid234"
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20201026, biz='1|4',
         |  uuid='$uuidIn')
         |select 'd5,斑马ai' as data
         |union all
         |select 'd5,斑马aI' as data
         |union all
         |select 'd5,斑马AI' as data
       """.stripMargin)

    val myJSON =
      s"""{
    "jobId":"test",
    "jobName":"crowd_app_time_filter",
    "day":"20201026",
    "rpcHost": null,
    "userId": "145",
    "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"uuid",
                    "uuid":"$uuidIn",
                    "value" : "$uuidIn",
                    "sep":",",
                    "idx":1,
                    "rules": [{
                        "include": 1,
                        "timeInterval":["20200521","20201026"],
                        "appStatus":0,
                        "numberRestrict":"1",
                        "appType":1
                     }],
                    "appListIndex":2,
                    "appListSep":"#"
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid":"$uuidOut",
                "limit":2000
            }
        }
    ]
  }"""

    CrowdAppTimeFilter.main(Array(myJSON))

    val cacheNewDF = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin)
    cacheNewDF.show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

    assertResult(3)(cacheNewDF.count())
    assertResult(Array("d5"))(cacheNewDF.collect().map(_.getAs[String]("data")).distinct)

  }

  test("测试 app_name/pkg 不区分大小写") {
    val uuidIn = "uuid123"
    val uuidOut = "uuid234"
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20201026, biz='1|4',
         |  uuid='$uuidIn')
         |select 'd5' as data
         |union all
         |select 'd5' as data
         |union all
         |select 'd5' as data
       """.stripMargin)

    val myJSON =
      s"""{
    "jobId":"test",
    "jobName":"crowd_app_time_filter",
    "day":"20201026",
    "rpcHost": null,
    "userId": "145",
    "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"uuid",
                    "uuid":"$uuidIn",
                    "value" : "$uuidIn",
                   "rules": [{
                       "include": 1,
                       "timeInterval":["20200521","20201015"],
                       "appStatus":0,
                       "appList":["斑马AI"],
                       "numberRestrict":"1,3",
                       "appType":1
                    }]
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid":"$uuidOut",
                "limit":2000
            }
        }
    ]
  }"""

    CrowdAppTimeFilter.main(Array(myJSON))

    val cacheNewDF = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut'
      """.stripMargin)
    cacheNewDF.show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut'
      """.stripMargin).show(false)

    assertResult(3)(cacheNewDF.count())
    assertResult(Array("d5"))(cacheNewDF.collect().map(_.getAs[String]("data")).distinct)

    val uuidOut2 = "uuid235"
    val myJSON2 =
      s"""{
    "jobId":"test",
    "jobName":"crowd_app_time_filter",
    "day":"20201026",
    "rpcHost": null,
    "userId": "145",
    "rpcPort": 0,
    "params":[
        {
            "inputs":[
                {
                    "inputType":"empty",
                    "uuid":"$uuidIn",
                    "value" : "$uuidIn",
                    "rules" : [{
                         "include": 1,
                         "timeInterval":["20200521","20201015"],
                         "appStatus":0,
                         "appList":["pkg-1"],
                         "numberRestrict":"1,3",
                         "appType":2
                    }]
                }
            ],
            "output":{
                "hdfsOutput":"",
                "module":"demo",
                "uuid":"$uuidOut2",
                "limit":2000
            }
        }
    ]
  }"""

    CrowdAppTimeFilter.main(Array(myJSON2))

    val cacheNewDF2 = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = '$uuidOut2'
      """.stripMargin)
    cacheNewDF2.show(false)

    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid = '$uuidOut2'
      """.stripMargin).show(false)

    assertResult(5)(cacheNewDF2.count())
  }

  test("添加多rule节点") {
    spark.sql(s"""TRUNCATE table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}""".stripMargin)
    Seq(
      ("张三", "com.jingdong", "京东", Seq(0, 2147483647, 0, 0, 0, 1048575, 0, 0, 0, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), "20210301", "20210312"),
      ("张三", "com.baidu", "百度", Seq(0, 0, 0, 0, 3, 0, 1073741823, 0, 0, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), "20210301", "20210312"),

      ("李四", "com.jingdong", "京东", Seq(0, 0, 0, 0, 0, 524287, 0, 0, 0, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), "20210301", "20210312"),
      ("李四", "com.zhifubao", "支付宝", Seq(0, 0, 0, 0, 0, 2147483647, 1073741823, 2147483647, 2147483647, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), "20210301", "20210312"),
      ("李四", "com.qq", "QQ", Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 1073741823, 2147483647, 1073741823, 1)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        , Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), "20210301", "20210312")
    ).toDF(
      "device", "pkg", "app_name", "installed", "active", "uninstall", "new_install", "upper_time", "day"
    ).write
      .insertInto(PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll)

    val mayJson =
      s"""
         |{
         |    "jobId":"profile_cal_1",
         |    "jobName":"crowd_app_time_filter",
         |    "day":"20190624",
         |    "rpcHost": null,
         |    "rpcPort": 0,
         |    "params":[
         |        {
         |            "inputs":[
         |                {
         |                    "inputType":"empty",
         |                    "uuid": null,
         |                    "compressType":"tgz",
         |                    "value":"张三:京东",
         |                    "appType":1,
         |                    "appListIndex":2,
         |                    "appListSep":",",
         |                    "sep":":",
         |                    "rules":[
         |                        {
         |                        "include":1,
         |                        "timeInterval":["20200501","20200601"],
         |                        "appStatus": 0,
         |                        "appList":["京东"],
         |                        "numberRestrict": "0",
         |                        "appType":1
         |                        },{
         |                        "include":0,
         |                        "timeInterval":["20200402","20200530"],
         |                        "appStatus": 0,
         |                        "appList":["百度"],
         |                        "numberRestrict": "1",
         |                        "appType":1
         |                        }
         |                    ]
         |                }
         |            ],
         |		"output": {
         |			"uuid": "481537486785425408_e2de2d4ed98c483c8c3b0d4a150d0e67",
         |			"value": "",
         |			"module": "ad_marketing",
         |			"keepSeed": 0
         |		}
         |        }
         |    ]
         |
         |}
       """.stripMargin

    CrowdAppTimeFilter.main(Array(mayJson))

    val resultTable = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
       """.stripMargin)
    resultTable.show(false)
    val res = resultTable.map(_.getString(0)).collect()
    assert(res.size == res.intersect(Seq("李四")).length)
  }
}
