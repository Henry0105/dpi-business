package com.mob.dataengine.engine.core.mapping

import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils

/*
  * @author chenfq
  */
class LocationDeviceMappingTest extends FunSuite with LocalSparkSession{

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  val json = FileUtils.getJson("unittest/mapping/location_device_mapping.json")
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._

    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = "CREATE DATABASE rp_mobdi_app")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "CREATE DATABASE rp_dataengine")

    val dataOPTCacheSql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)

    val locationHomeWorkSql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_location/location.sql",
        tableName = PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK
      )
    createTable(locationHomeWorkSql)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK} partition(day='20190401')
         |  select
         |  "b8ca951f21f5b398dba5fe8637d2ba8532ff28c7" as device,
         |  	"119.30053200276217" as lon_home,
         |   	"26.042652619332458" as lat_home,
         |    	"1" as cluster_home,
         |     	"5" as cnt_home,
         |      	"0.0" as max_distance_home,
         |       	"0.0" as min_distance_home,
         |        	"0.5" as confidence_home,
         |         "119.30053200276217" as lon_work,
         |         	"26.042652619332458" as lat_work,
         |          	"2" as cluster_work,
         |           	"6" as cnt_work,
         |            	"55.95142501649571" as max_distance_work,
         |             	"14.870630475473842" as min_distance_work,
         |              	"0.8"  as confidence_work,
         |               	1 as type,
         |                	"cn" as country_home,
         |                 	"cn3" as province_home,
         |                  	"cn3_04_83" as area_home,
         |                   "中国" as country_cn_home,
         |                   	"浙江" as province_cn_home,
         |                    	"路桥区" as area_cn_home,
         |                     	"cn" as country_work,
         |                       	"cn3" as province_work,
         |                        	"cn3_04_83" as area_work,
         |                         	"中国" as country_cn_work,
         |                          	"浙江" as province_cn_work,
         |                           	"路桥区" as area_cn_work,
         |                            	"cn3_04" as city_home,
         |                             	"台州" as city_cn_home,
         |                              	"cn3_04" as city_work,
         |                               	"台州" as city_cn_work
       """.stripMargin)


    val df = Seq(
      // 第一个设备
      ("b76f6cf7432c6a670d7e8c790ef3d5659caa043e", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301"),
      ("b76f6cf7432c6a670d7e8c790ef3d5659caa043e", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301"),
      ("b76f6cf7432c6a670d7e8c790ef3d5659caa043e", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301"),
      // 第二个设备
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301"),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301"),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301"),
      // 第三个设备
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301"),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301"),
      ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", 119.30053200276217, 26.042652619332458, 2,
        "cn", "cn3", "cn3_04", "cn3_04_83", "4", "20190301")
    ).toDF(
      "device", "lon", "lat", "cnt", "country", "province", "city", "area", "rank", "day"
    )
    val locationFrequencySql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_mobdi_app/rp_device_frequency_3monthly.sql",
      tableName = PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY
    )
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    createTable(locationFrequencySql)
    insertDF2Table(df, PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY, Some("day"), false)
    spark.table(PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY).show(false)
  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
  }

  test(testName = "location_device_mapping_test") {
    LocationDeviceMapping.main(Array(json))

    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).show(false)

    val res = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = 'out-123123'
       """.stripMargin)
    res.show(false)

    assert(res.count().toInt == 3)
  }
}
