package com.mob.dataengine.utils.iostags

import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.traits.Logging
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.iostags.beans.Param
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
 * @author xlmeng
 */
object IosTagsGeneratorFull {

  def main(args: Array[String]): Unit = {
    val defaultParams = Param()
    val projectName = s"TagsGenerator[${DateUtils.getCurrentDay()}]"
    val parser = new OptionParser[Param](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .text(s"tag更新时间")
        .action((x, c) => c.copy(day = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(p) =>
        println(p)
        val spark: SparkSession = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()
        IosTagsGeneratorFull(spark, p).run()
        spark.close()
      case _ =>
        println(s"参数有误:${args.mkString(",")}")
        sys.exit(1)
    }
  }

}

case class IosTagsGeneratorFull(@transient spark: SparkSession, p: Param) extends Logging {

  val tagTfidProfileIdSet: Set[String] = MetadataUtils.findTaglistLikeProfiles("tag_tfidf;").values.toSet
  val tagTfidProfileIdSetBC: Broadcast[Set[String]] = spark.sparkContext.broadcast(tagTfidProfileIdSet)

  def run(): Unit = {
    val day = p.day
    val pday = DateUtils.minusDays(p.day, 1)

    spark.udf.register("update_maparr", (fullMap: Map[String, Seq[String]],
                                         updateMap: Map[String, Seq[String]], day: String) => {
      if (null == updateMap || updateMap.isEmpty) {
        fullMap
      } else if (null == fullMap || fullMap.isEmpty) {
        updateMap.mapValues(_.:+(day))
      } else {
        filterListTags(fullMap, updateMap) ++ updateMap.mapValues(_.:+(day))
      }
    })


    sql(
      s"""
         |SELECT ifid
         |     , first(tags, true) as tags
         |     , first(confidence, true) as confidence
         |     , first(update_tags, true) as update_tags
         |     , first(update_confidence, true) as update_confidence
         |     , max(update_time) as update_time
         |FROM (
         |      SELECT ifid
         |           , null as tags, null as confidence
         |           , tags as update_tags, confidence as update_confidence
         |           , day as update_time
         |      FROM ${PropUtils.HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_DI_SEC}
         |      WHERE day = '$day'
         |
         |      UNION ALL
         |
         |      SELECT ifid
         |           , tags, confidence
         |           , null as update_tags, null as update_confidence
         |           , update_time
         |      FROM ${PropUtils.HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_FULL_SEC}
         |      WHERE day = '$pday'
         |     )
         |GROUP BY ifid
         |""".stripMargin).createOrReplaceTempView("t1")

    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_FULL_SEC} partition(day = '$day')
         |select ifid
         |     , update_maparr(tags, update_tags, '$day') as tags
         |     , update_maparr(confidence, update_confidence, '$day') as confidence
         |     , update_time
         |from t1
         |""".stripMargin
    )
  }


  // 剔除taglist和catelist全部
  def filterListTags(fullMap: Map[String, Seq[String]],
                     updateMap: Map[String, Seq[String]]): Map[String, Seq[String]] = {
    val tagTfidProfileIdSet = tagTfidProfileIdSetBC.value
    if ((updateMap.keySet & tagTfidProfileIdSet).nonEmpty) {
      fullMap.filterNot { case (k, _) => tagTfidProfileIdSet.contains(k) }
    } else {
      fullMap
    }
  }


}
