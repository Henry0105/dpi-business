package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: zhangnw
 * Date: 2018-07-31
 * Time: 14:46
 */
object DeviceMappingBloomFilter {

  /**
   * 过滤DEVICE_MAPPING
   * @param spark spark
   * @param deviceDF 输入device
   * @return
   */
  private def deviceBloomFilter(spark: SparkSession, deviceDF: DataFrame): DataFrame = {
    val deviceInDf = deviceDF.select("device").distinct()
    val deviceBloomFilter = deviceInDf.coalesce(20).stat.bloomFilter("device", deviceInDf.count(), 0.0001)
    val deviceMappingTbName: String = PropUtils.HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING
    spark.sql(
      s"""
         | SELECT
         | *
         | FROM $deviceMappingTbName
         | WHERE device IS NOT NULL AND device <> '' and applist <> 'unknown' and applist is not null
      """.stripMargin)
      .filter(row =>
        deviceBloomFilter.mightContainString(row.getAs("device").asInstanceOf[String].trim.toLowerCase()))
  }

  /**
   * device mapping
   * @param spark spark
   * @param deviceDf 输入的device
   * @return
   */
  def deviceMapping(spark: SparkSession, deviceDf: DataFrame): DataFrame = {

    deviceDf.createOrReplaceTempView("device_df_tmp")
    deviceBloomFilter(spark, deviceDf)
      .createOrReplaceTempView("device_mapping_df_tmp")

    val joinSQL =
      s"""
         |select
         | coalesce(b.device,'-1') device
         |,a.userid
         |,a.userid AS storeid
         |,a.rank_date
         |,'2' data_type
         |,'0' encrypt_type
         |,coalesce(country,'unknown') as country
         |,coalesce(province,'unknown') as province
         |,coalesce(city,'unknown') as city
         |,coalesce(gender,-1) as gender
         |,coalesce(agebin,-1) as agebin
         |,coalesce(segment,-1) as segment
         |,coalesce(edu,-1) as edu
         |,coalesce(kids,-1) as kids
         |,coalesce(income,-1) as income
         |,coalesce(cell_factory,'unknown') as cell_factory
         |,coalesce(model,'unknown') as model
         |,coalesce(model_level,-1) as model_level
         |,coalesce(carrier,'unknown') as carrier
         |,coalesce(network,'unknown') as network
         |,coalesce(screensize,'unknown') as screensize
         |,coalesce(sysver,'unknown') as sysver
         |,coalesce(case trim(permanent_country) when '' then null else permanent_country end,country,'unknown')
         | as permanent_country
         |,coalesce(case trim(permanent_province) when '' then null else permanent_province end,province,'unknown')
         | as permanent_province
         |,coalesce(case trim(permanent_city) when '' then null else permanent_city end,city,'unknown')
         | as permanent_city
         |,coalesce(occupation,-1) as occupation
         |,coalesce(industry,-1) as industry
         |,coalesce(house,-1) as house
         |,coalesce(repayment,-1) as repayment
         |,coalesce(car,-1) as car
         |,coalesce(workplace,'unknown') as workplace
         |,coalesce(residence,'unknown') as residence
         |,coalesce(married,-1) as married
         |,applist
         |,group_list
         |,match_flag
         | FROM device_df_tmp a
         | LEFT JOIN device_mapping_df_tmp b
         | ON a.device=b.device
      """.stripMargin

    spark.sql(joinSQL)
  }

}
