package com.mob.dataengine.commons

import com.mob.dataengine.commons.pojo.ParamOutput
import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author juntao zhang
 */
object DeviceCacheWriterNew {

  def insertTable(spark: SparkSession, output: ParamOutput, dataset: DataFrame, biz: String): Unit = {
    import spark._

    sql(
      s"""
         |ALTER TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |DROP IF EXISTS PARTITION(uuid='${output.getUUID}')
       """.stripMargin).show()

    dataset.createOrReplaceTempView("t")

    sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |PARTITION(day=${DateUtils.currentDay()},uuid='${output.getUUID}')
         |SELECT id, match_ids, id_type, encrypt_type
         |FROM t
           """.stripMargin)
  }

}
