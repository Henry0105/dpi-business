package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils
import com.mob.dataengine.engine.core.crowd.CrowdFilter
import org.apache.spark.sql.LocalSparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest._

/**
 * @author juntao zhang
 */
class CrowdFilterTest extends FunSuite with LocalSparkSession {
  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("create database dm_dataengine_tags")
    spark.sql("create database rp_dataengine")
    val dataOPTCacheNewSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      "rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)
    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    //    stop()
  }

  def prepareWarehouse(): Unit = {
    Seq(
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf0", 5, "1", "HUAWEI", "7", "1", "5", "2", "MHA-AL00", "0", "15",
        "A08,B0P,615,594,263,B2S,334", "7015_019,7005_001", "cn", "cn5", "cn5_10",
        "", "", "", "", "", "ph.com.smart.netphone,apps.redpi.touchscreenrepair"
      ),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c", 5, "1", "HUAWEI", "7", "1", "5", "2", "MHA-AL00", "0", "15",
        "A08,B0P,615,594,263,B2S,334", "7015_019,7005_001", "cn", "cn5", "cn5_10",
        "", "", "", "", "", "ph.com.smart.netphone,apps.redpi.touchscreenrepair"
      ),
      ("04399d01c2e2fac798f4f1b47285185ebbdca738", 5, "1", "HUAWEI", "7", "1", "5", "2", "MHA-AL00", "0", "15",
        "A08,B0P,615,594,263,B2S,334", "7015_019,7005_001", "cn", "cn5", "cn5_10",
        "", "", "", "", "", "ph.com.smart.netphone,apps.redpi.touchscreenrepair"
      ),
      ("0001e31e886e7885f2bd48bde2ec22583a2cc1fb", 5, "1", "HUAWEI", "7", "1", "5", "2", "MHA-AL00", "0", "15",
        "A08,B0P,615,594,263,B2S,334", "7015_019,7005_001", "cn", "cn5", "cn5_10",
        "", "", "", "", "", "ph.com.smart.netphone,apps.redpi.touchscreenrepair"
      ),

      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf02", 5, "1", "HUAWEI", "7", "1", "5", "2", "MHA-AL00", "0", "15",
        null, "7015_019,7005_001", "cn", "cn5", "cn5_10",
        "", "", "", "", "", "ph.com.smart.netphone,apps.redpi.touchscreenrepair"
      ),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c2", 5, "1", "HUAWEI", "7", "1", "5", "2", "MHA-AL00", "0", "15",
        null, "7015_019,7005_001", "cn", "cn5", "cn5_10",
        "", "", "", "", "", "ph.com.smart.netphone,apps.redpi.touchscreenrepair"
      ),
      ("04399d01c2e2fac798f4f1b47285185ebbdca7382", 5, "1", "HUAWEI", "7", "1", "5", "2", "MHA-AL00", "0", "15",
        "PLK", "7015_019,7005_001", "cn", "cn5", "cn5_10",
        "", "", "", "", "", "ph.com.smart.netphone,apps.redpi.touchscreenrepair"
      ),
      ("0001e31e886e7885f2bd48bde2ec22583a2cc1fb2", 5, "1", "HUAWEI", "7", "1", "5", "2", "MHA-AL00", "0", "15",
        "A08,B0P,615,594,263,B2S,334", "7015_019,7005_001", "cn", "cn5", "cn5_10",
        "", "", "", "", "", "ph.com.smart.netphone"
      )
    ).toDF(
      "device",
      "agebin",
      "car",
      "cell_factory",
      "edu",
      "gender",
      "income",
      "kids",
      "model",
      "model_level",
      "occupation",
      "tag_list",
      "catelist",
      "country",
      "province",
      "city",
      "permanent_country",
      "permanent_province",
      "work_geohash",
      "home_geohash",
      "frequency_geohash_list",
      "applist"
    ).withColumn("permanent_city", lit(""))
      .write.saveAsTable("dm_dataengine_tags.dm_device_profile_full_enhance_view")

    Seq(
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf02", "20190401", "cf|4", "2756620188844154882"),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c2", "20190401", "cf|4", "2756620188844154882"),
      ("04399d01c2e2fac798f4f1b47285185ebbdca7382", "20190401", "cf|4", "2756620188844154882")
    ).toDF(
      "data", "created_day", "biz", "uuid"
    ).write.partitionBy("created_day", "biz", "uuid")
      .saveAsTable(PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
  }

  test("crowd filter test") {
    val myJSON = FileUtils.getJson(s"unittest/crowd_filter" +
      s"/crowd_filter_python_new.json")

    CrowdFilter.main(Array(myJSON))
    val param = CrowdFilter.jobContext.params.head
    println(param)

    val resCnt = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = ${param.output.uuid}
      """.stripMargin).count()
    assert(1 == resCnt)
  }

  test("通过包名过滤出device") {
    val js =
      s"""
         |{
         |  "jobId": "mobeye_enterprise_customerdata_market_dmp_20190509_289784967016075264",
         |  "projectName": "mobeye",
         |  "rpcHost": null,
         |  "userId": "145",
         |  "rpcPort": 0,
         |  "params": [
         |    {
         |      "inputs": [
         |        {
         |          "uuid": "",
         |          "include": {
         |            "applist": "apps.redpi.touchscreenrepair"
         |          },
         |          "exclude": {}
         |        }
         |      ],
         |      "output": {
         |        "hdfs_output": "/tmp/dataengine/schedule/mobeye_enterprise_customerdata_market_dmp_20190509_289784967016075264/0",
         |        "uuid": "289784967016075264"
         |      }
         |    }
         |  ],
         |  "jobName": "crowd_filter",
         |  "day": "20190510"
         |}
       """.stripMargin

    CrowdFilter.main(Array(js))
    val param = CrowdFilter.jobContext.params.head
    println(param)

    val resCnt = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = ${param.output.uuid}
      """.stripMargin).count()
    assert(7 == resCnt)
  }
}
