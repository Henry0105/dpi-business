package com.mob.dataengine.utils.hbase


import com.mob.dataengine.commons.enums.OSName
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.hbase.helper.TagsHFileGeneratorCommon
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xlmeng
 */
case class IosTagsHFileGenerator(@transient override val spark: SparkSession, day: String)
  extends TagsHFileGeneratorCommon(spark, day) {

  override def transformData(full: Boolean): DataFrame = {
    val profileIdToLevel2CategoryMockId = profileIdToLevel2CategoryId.map {
      case (k, v) =>
        val mockid = if (tagTfidfProfileIdArray.contains(k)) {
          tagTdifdCategoryIdMock
        } else {
          v
        }
        (k, mockid)
    }

    tagTfidfCategoryIdsBroadcast = taglistLikeCategoryIdBroadcast(tagTfidfProfileIdArray, profileIdToLevel2CategoryId)

    profileIdToLevel2CategoryIdBC = spark.sparkContext.broadcast(profileIdToLevel2CategoryId)
    profileIdToLevel2CategoryMockIdBC = spark.sparkContext.broadcast(profileIdToLevel2CategoryMockId)

    spark.udf.register("filter_by_day_cateid", filterByDayCateId(day) _)
    spark.udf.register("filter_by_day", filterByDay(day) _)
    spark.udf.register("divide_by_cateid", divideByCateid(full) _)
    spark.udf.register("map_value_trans_2", mapValueTrans(_: Map[String, Map[String, Seq[String]]]))
    spark.udf.register("fill_taglist", fillTaglist(OSName.IOS) _)

    sql(
      s"""
         |select ifid
         |     , ${if (full) "" else s"filter_by_day_cateid(tags)"} tags
         |     , ${if (full) "" else s"filter_by_day(confidence)"} confidence
         |from (
         |      select ifid
         |           , divide_by_cateid(tags) tags
         |           , confidence
         |      from ${PropUtils.HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_FULL_SEC}
         |      where day = '$day'
         |            ${if (full) "" else s"and update_time = '$day'"}
         |     ) as a
        """.stripMargin).createOrReplaceTempView("expand_tmp_1")

    sql(
      s"""
         |select ifid
         |     , map_concat(nvl(tags, map()), nvl(confidence, map()), map('update_time', '$day')) as profile
         |from  (
         |       select ifid
         |            , map_value_trans_2(tags) tags
         |            , map_value_trans_2(confidence) confidence
         |       from  (
         |              select ifid
         |                   , ${if (full) "" else s"fill_taglist(tags, '$day')"} tags
         |                   , map('confidence', confidence) confidence
         |              from  expand_tmp_1
         |              where tags is not null
         |             ) as b
         |     ) as c
         |where tags is not null
         |""".stripMargin).createOrReplaceTempView("expand_tmp_2")

      sql(
        s"""
           |select ifid
           |     , first(profile) as profile
           |from expand_tmp_2
           |group by ifid
           |""".stripMargin)
  }
}