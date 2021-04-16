package com.mob.dataengine.utils.hbase

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.hbase.helper.TagsHFileGeneratorCommon
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xlmeng
 */
case class AndroidTagsHFileGenerator(@transient override val spark: SparkSession, day: String)
  extends TagsHFileGeneratorCommon(spark, day) {

  def transformData(full: Boolean): DataFrame = {
    profileIdToLevel2CategoryIdBC = spark.sparkContext.broadcast(profileIdToLevel2CategoryId)

    spark.udf.register("divide_by_cateid", divideByCateid(isNotMock = true) _)
    spark.udf.register("filter_by_day_cateid", filterByDayCateId(day) _)
    spark.udf.register("filter_by_day", filterByDay(day) _)
    spark.udf.register("map_value_trans", mapValueTrans(_: Map[String, Map[String, Seq[String]]]))

    val nvlColumns = Seq("confidence", "tag_list", "catelist", "tags")
      .map { fieldName => s"nvl($fieldName, map())" }.mkString(", ")
    sql(
      s"""
         |select device
         |     , map_value_trans(map('confidence', confidence)) as confidence
         |     , map_value_trans(map('tag_list', tag_list)) as tag_list
         |     , map_value_trans(map('catelist', catelist)) as catelist
         |     , map_value_trans(tags) as tags
         |from (
         |      select device
         |           , ${if (full) "" else s"filter_by_day(confidence)"} confidence
         |           , ${if (full) "" else s"filter_by_day(tag_list)"} tag_list
         |           , ${if (full) "" else s"filter_by_day(catelist)"} catelist
         |           , ${if (full) "divide_by_cateid(tags)" else s"filter_by_day_cateid(divide_by_cateid(tags))"} tags
         |      from ${PropUtils.HIVE_TABLE_PROFILE_TAGS_INFO_FULL_V2}
         |      where day = '$day'
         |            ${if (full) "" else s"and update_time = '$day'"}
         |            and device rlike '[0-9a-f]{40,40}' and length(device) = 40 and length(day)=8
         |     ) as a
         |""".stripMargin).createOrReplaceTempView("expand_tmp_1")

    sql(
      s"""
         |select device, profile
         |from (
         |      select device
         |           , map_concat($nvlColumns, map('update_time', '$day')) as profile
         |      from expand_tmp_1
         |     ) as c
         |where cardinality(profile) > 1
         |""".stripMargin)
  }

}