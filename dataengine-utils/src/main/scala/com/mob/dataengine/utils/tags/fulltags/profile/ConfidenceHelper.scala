package com.mob.dataengine.utils.tags.fulltags.profile

import org.apache.spark.sql.{DataFrame, SparkSession}

object ConfidenceHelper {

  import com.mob.dataengine.utils.tags.fulltags.profile.FullTagsGeneratorHelper.{kvSep, pairSep, profileConfidence}
  import FullTagsGeneratorHelper.sql

  /**
   * 计算出每个device的置信度信息
   */
  def buildAllProfileConfidence(spark: SparkSession, sample: Boolean): Unit = {
    import spark.implicits._
    val confidences = sql(spark,
      s"""
         |select
         |  profile_id,
         |  profile_version_id,
         |  profile_database,
         |  profile_table,
         |  profile_column,
         |  confidence_name
         |from $profileConfidence
      """.stripMargin)
      .map(r =>
        ProfileConfidence(
          r.getAs[Int]("profile_id"),
          r.getAs[Int]("profile_version_id"),
          r.getAs[String]("profile_database"),
          r.getAs[String]("profile_table"),
          r.getAs[String]("profile_column"))
      ).collect().groupBy(_.profileTable)

    val confidenceDf: DataFrame = confidences.mapValues(c => {
      sql(spark,
        s"""
           |select
           |  device,
           |  concat_ws('$pairSep', ${FullTagsGeneratorHelper.buildMapStringFromFields(c, kvSep)})
           |  as confidence,
           |  processtime as update_time
           |from ${c.head.fullTableName}
        """.stripMargin)
    }).values.reduce(_ union _)

    confidenceDf.createOrReplaceTempView("device_confidence_tmp")

  }
}
