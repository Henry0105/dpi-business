package com.mob.dataengine.commons.traits

import com.mob.dataengine.commons.enums.DeviceType.{DEVICE, DeviceType, IDFA, IMEI, IMEI14, IMEI15, IMSI, MAC, OAID, PHONE, SERIALNO}
import com.mob.dataengine.commons.enums.SecDeviceType._
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.SparkSession

trait TableTrait extends Logging {
  /**
   * 获取最新分区
   *
   * @param table    : 表名
   * @param idx      : 第几个分区字段,从0开始 day=20200101/flag=1/timewindow=7 类似这样的多分区
   * @param filterBy : 分区过滤器, 多分区的时候可以指定,例如取flag=1,timewindow=7的最新的day
   *                 return: day=20200201
   */
  def getLastPar(table: String, idx: Int = 0, filterBy: String => Boolean = _ => true): String = {
    sql(s"show partitions $table").collect().map(_.getString(0))
      .filter(filterBy).map(_.split("/")(idx)).max
  }

  /**
   * 是否有某个分区
   *
   * @param table    : 表名
   * @param filterBy : 分区过滤器
   */
  def hasPar(table: String, filterBy: String => Boolean = _ => true): Boolean = {
    sql(s"show partitions $table").collect().map(_.getString(0)).exists(filterBy)
  }


  /**
   * 对分区表的某个分区建立view
   */
  def createView(tableName: String, versionVal: String, versionName: String = "version"): Unit = {
    val schema = spark.table(tableName).schema

    val fields = schema.fields.map(f => {
      f.name
    }).filterNot(_.equals(versionName))

    val query =
      s"""
         |create or replace view ${tableName}_view
         |(
         | ${fields.mkString(",")}
         |) COMMENT ''
         |as select
         |  ${fields.mkString(",")}
         |from $tableName where $versionName='$versionVal'
    """.stripMargin
    println(query)
    sql(query)
  }

  /** 获得对应的mapping表 */
  def getSrcTable(idType: DeviceType, deviceMatch: Int = 0): String = {
    idType match {
      case PHONE => deviceMatch match {
        case 2 => PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK_VIEW
        case _ => PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_VIEW
      }
      case MAC => PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW
      case DEVICE => PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3_VIEW
      case IMEI => PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3_VIEW
      case IMEI14 => PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3_VIEW // 都走imei的表,不走imei14的表
      case IMEI15 => PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3_VIEW
      case IDFA => PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3_VIEW
      case IMSI => PropUtils.HIVE_TABLE_DM_IMSI_MAPPING_V3_VIEW
      case SERIALNO => PropUtils.HIVE_TABLE_DM_SERIALNO_MAPPING_V3_VIEW
      case OAID => PropUtils.HIVE_TABLE_DM_OAID_MAPPING_V3_VIEW
    }
  }


  /** 获得对应的mapping表 */
  def getSecSrcTable(idType: SecDeviceType, deviceMatch: Int = 0): String = {
    idType match {
      case IEID => PropUtils.HIVE_TABLE_IEID_FULL
      case MCID => PropUtils.HIVE_TABLE_MCID_FULL
      case PID => PropUtils.HIVE_TABLE_PID_FULL
      case IFID => PropUtils.HIVE_TABLE_IFID_FULL
      case ISID => PropUtils.HIVE_TABLE_ISID_FULL
      case SNID => PropUtils.HIVE_TABLE_SNID_FULL
      case OIID => PropUtils.HIVE_TABLE_OIID_FULL
    }
  }
}
