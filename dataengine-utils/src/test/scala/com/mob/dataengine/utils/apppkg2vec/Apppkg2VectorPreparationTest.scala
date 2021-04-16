package com.mob.dataengine.utils.apppkg2vec

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.commons.utils.PropUtils._
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class Apppkg2VectorPreparationTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database rp_dataengine")
    spark.sql("create database dm_sdk_mapping")
    spark.sql("create database rp_mobdi_app")

    val dataSql0 = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/" +
      "apppkg_vector_mapping.sql", tableName = HIVE_TABLE_APPPKG_VECTOR_MAPPING)
    createTable(dataSql0)

    val dataSql1 = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      "dm_sdk_mapping/app_category_mapping_par.sql", tableName = HIVE_TABLE_APP_CATEGORY_MAPPING_PAR)
    createTable(dataSql1)

    val dataSql2 = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      "dm_sdk_mapping/rp_app_name_info.sql", tableName = HIVE_TABLE_RP_APP_NAME_INFO)
    createTable(dataSql2)

    val dataSql3 = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      "rp_mobdi_app/apppkg_icon2vec_par_wi.sql", tableName = HIVE_TABLE_APPPKG_ICON2VEC_PAR_WI)
    createTable(dataSql3)

    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
  }

  def prepareWarehouse(): Unit = {
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    // 1.dm_sdk_mapping.app_category_mapping_par
    val df1 = Seq(
      ("com.zzlywgl.h5.twly", "com.zzlywgl.h5.twly", "贪玩蓝月", "游戏服务", "MMO", "7019", "7019_102", "1000.20200306"),
      ("com.tanwan.mobile.wzcq", "com.tanwan.mobile.wzcq", "贪玩蓝月", "游戏服务", "MMO", "7019", "7019_102", "1000.20200306"),
      ("com.tencent.tmgp.twly", "com.tencent.tmgp.twly", "贪玩蓝月", "游戏服务", "MMO", "7019", "7019_102", "1000.20200306"),
    ).toDF("pkg", "apppkg", "appname", "cate_l1", "cate_l2", "cate_l1_id", "cate_l2_id", "version")
    insertDF2Table(df1, HIVE_TABLE_APP_CATEGORY_MAPPING_PAR, Some("version"), false)

    // 2.dm_sdk_mapping.rp_app_name_info
    val df2 = Seq(
      ("com.zzlywgl.h5.twly", "贪玩蓝月", 50, "20200114"),
    ).toDF("apppkg", "app_name", "cnt", "day")
    insertDF2Table(df1, HIVE_TABLE_RP_APP_NAME_INFO, None)

    // 3.rp_mobdi_app.apppkg_icon2vec_par_wi
    val df3 = Seq(
      ("贪玩蓝月", "贪玩蓝月_1586790313232.jpg", "0.498017460108,1.35636758804,0.000711406464688", "20200401", "taptap"),
      ("贪玩蓝月", "贪玩蓝月_1586790313232.jpg", "0.498017460108,1.35636758804,0.000711406464688", "20200401", "dysy")
    ).toDF("app_name", "image_name", "features", "day", "source_type")
    insertDF2Table(df1, HIVE_TABLE_APPPKG_ICON2VEC_PAR_WI, Some("day, source_type"), false)
  }

  test("游戏竞品预计算 单元测试") {
    val args = Array("--day 20200429", "--cnt 50", "--day 20200429", "--rpcPort localhost", "--rpcPort 9090")

    Apppkg2VectorPreparation.main(args)

  }
}
