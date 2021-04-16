package com.mob.dataengine.commons

import com.mob.dataengine.commons.pojo.ParamOutput
import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author juntao zhang
 */
object DeviceCacheWriter {
  def insertTable(spark: SparkSession, output: ParamOutput, dataset: DataFrame, biz: String): Unit = {
    import spark._

    sql(
      s"""
         |ALTER TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |DROP IF EXISTS PARTITION(uuid='${output.getUUID}', biz='$biz')
       """.stripMargin).show()
    dataset.createOrReplaceTempView("t")
    sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |PARTITION(created_day=${output.day.get},biz='$biz',uuid='${output.getUUID}')
         |SELECT data FROM t
           """.stripMargin
    )
  }

  def insertTable2(spark: SparkSession, jobCommon: JobCommon,
    output: JobOutput, dataset: DataFrame, biz: String): Unit = {
    import spark._

    sql(
      s"""
         |ALTER TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |DROP IF EXISTS PARTITION(uuid='${output.uuid}', biz='$biz')
       """.stripMargin).show()
    dataset.createOrReplaceTempView("t")
    sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |PARTITION(created_day=${jobCommon.day},biz='$biz',uuid='${output.uuid}')
         |SELECT data FROM t
           """.stripMargin
    )
  }

}
