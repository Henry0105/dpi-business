package com.mob.dataengine.utils.tags.profile

import org.apache.spark.sql.{DataFrame, SparkSession}

object ConfidenceHelper {

  import com.mob.dataengine.utils.tags.profile.TagsGeneratorHelper.{kvSep, pairSep, profileConfidence}
  import TagsGeneratorHelper.sql

  /**
   * 计算出每个device的置信度信息
   */
  def buildProfileConfidence(spark: SparkSession, sample: Boolean): Unit = {
    import spark.implicits._
    val confidences = sql(spark,
      s"""
         |select
         |  c.profile_id,
         |  c.profile_version_id,
         |  c.profile_database,
         |  c.profile_table,
         |  c.profile_column,
         |  c.confidence_name
         |from $profileConfidence c
         |inner join
         |tmp_profileids_full f
         |on concat(c.profile_id, '_', c.profile_version_id) = f.profile_id
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
           |  concat_ws('$pairSep', ${TagsGeneratorHelper.buildMapStringFromFields(c, kvSep)})
           |  as confidence,
           |  processtime as update_time
           |from ${c.head.fullTableName}
        """.stripMargin)
    }).values.reduce(_ union _)

    confidenceDf.createOrReplaceTempView("device_confidence_tmp")
  }
}
