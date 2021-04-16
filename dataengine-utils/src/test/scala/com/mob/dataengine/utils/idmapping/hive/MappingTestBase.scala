package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.{DataFrame, Encoder, LocalSparkSession}
import org.scalatest.FunSuite
import com.mob.dataengine.utils.idmapping.IdMappingToolsV2._


class MappingTestBase extends FunSuite with LocalSparkSession{
  val day = "20190515"
  val yesterday = "20190514"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.udf.register("get_latest_elements", getLatestElements _)
    spark.udf.register("get_max_elements", getMaxElements _)
    spark.udf.register("aggregateDevice", new AggregateIdTypeV2(200))
    spark.udf.register("get_tm", (arr: Seq[String], idx: Int) => {
      if (null == arr) {
        null
      } else {
        arr.map(e => e.split("\u0001")(idx))
      }
    })
    spark.udf.register("md5_array", md5Array _)
    spark.udf.register("collapse_array", collapseArray _)
    spark.udf.register("combine_incr", combineIncr _)
    spark.udf.register("combine_device_plat", combineDevicePlat _)
    spark.udf.register("merge_list", mergeLists _)

    spark.conf.set("spark.sql.shuffle.partitions", 10)
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")

    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database dm_mobdi_mapping")
    spark.sql("create database dm_sdk_mapping")

    assert(PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3 != null)

    // dm_dataengine_mapping.dm_device_mapping_v3
    val dmDeviceMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_device_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3)
    createTable(dmDeviceMappingSql)

    // dm_dataengine_mapping.dm_device_mapping_v3_inc
    val dmDeviceMappingSql2 = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_device_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3_INC)
    createTable(dmDeviceMappingSql2)

    // dm_dataengine_mapping.id_mapping_external_full_inc
    val dmDeviceMappingSql3 = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_device_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC)
    createTable(dmDeviceMappingSql3)

    // dm_sdk_mapping.device_duid_mapping_new
    val deviceDuidMappingNewSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_sdk_mapping/device_duid_mapping_new.sql",
      tableName = PropUtils.HIVE_TABLE_DEVICE_DUID_MAPPING_NEW)
    createTable(deviceDuidMappingNewSql)

    // dm_mobdi_mapping.android_id_mapping_full_view
    val androidIdMappingViewSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_mobdi_mapping/android_id_mapping_view.sql",
      tableName = PropUtils.HIVE_ORIGINAL_ANDROID_ID_MAPPING_V2)
    createTable(androidIdMappingViewSql)

    // dm_mobdi_mapping.ios_id_mapping_full_view
    val iosIdMappingViewSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_mobdi_mapping/ios_id_mapping_view.sql",
      tableName = PropUtils.HIVE_ORIGINAL_IOS_ID_MAPPING_V2)
    createTable(iosIdMappingViewSql)

    // dm_dataengine_mapping.id_mapping_external_full_view
    val idMappingFullSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/id_mapping_external.sql", PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_VIEW)
    createTable(idMappingFullSql)

    prepareWarehouse()
  }

  def prepareWarehouse(): Unit = {
    CSVUtils.loadDataIntoHive(spark, "../data/mapping/android_id_mapping_view_v2",
      PropUtils.HIVE_ORIGINAL_ANDROID_ID_MAPPING_V2)
    CSVUtils.loadDataIntoHive(spark, "../data/mapping/ios_id_mapping_view_v2",
      PropUtils.HIVE_ORIGINAL_IOS_ID_MAPPING_V2)
    CSVUtils.loadDataIntoHive(spark, "../data/mapping/device_duid_mapping_new",
      PropUtils.HIVE_TABLE_DEVICE_DUID_MAPPING_NEW)
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
  }

  // 根据主键查询特定字段
  def filterByKey[T: Encoder](df: DataFrame, keyField: String, keyValue: String, field: String): T = {
    import spark.implicits._

    df.filter(s"$keyField = '$keyValue'")
      .select(field).map(_.getAs[T](0)).collect().head
  }
}
