package com.mob.dataengine.utils.iostags

import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.iostags.beans.IosTagsGeneratorIncrParam
import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper
import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper.{kvSep, pairSep, sql}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * @author xlmeng
 */
class IosTagsGeneratorFullFirst(@transient  spark: SparkSession, p: IosTagsGeneratorIncrParam)
  extends IosTagsGeneratorIncr(spark, p) {

  import IosTagsGeneratorIncr._


  override def persist2Hive(): Unit = {
    val tagTfidProfileIdSet: Set[String] = MetadataUtils.findTaglistLikeProfiles("tag_tfidf;").values.toSet
    val tagTfidProfileIdSetBC: Broadcast[Set[String]] = spark.sparkContext.broadcast(tagTfidProfileIdSet)
    spark.udf.register("kv_str_to_map", new TagsGeneratorHelper.kvStr2mapFull)
    spark.udf.register("remove_old_tags_like", TagsGeneratorHelper.removeOldTagsLike(tagTfidProfileIdSetBC) _)

    sql(spark,
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_DI_SEC} PARTITION (day='${p.day}')
         |SELECT ifid
         |     , ${if (p.full) "remove_old_tags_like(tags)" else "tags"} as tags
         |     , confidence
         |FROM   (
         |        SELECT ifid
         |             , kv_str_to_map(kv, '$pairSep', '$kvSep', update_time) as tags
         |             , kv_str_to_map(confidence, '$pairSep', '$kvSep', update_time) as confidence
         |        FROM (
         |              SELECT ifid, kv, null as confidence, update_time
         |              FROM   $tagsTable
         |              UNION ALL
         |              SELECT ifid, null AS kv, confidence, update_time
         |              FROM   $confidenceTable
         |             ) res_1
         |        GROUP BY ifid
         |       ) res_2
         |WHERE  tags is not null
         |""".stripMargin)

    println("IOS标签增量表写入成功")
  }

}
