package com.mob.dataengine.utils.tags.fulltags


import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import scopt.OptionParser


object UpdateFullTags {

  case class Param(
                    day: String = ""
                  ) {

    import org.json4s.jackson.Serialization.write

    implicit val _ = DefaultFormats

    override def toString: String = write(this)(DefaultFormats)
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Param()
    val projectName = s"Update_full_tags[${DateUtils.getCurrentDay()}]"
    val parser = new OptionParser[Param](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .text(s"运行时间")
        .action((x, c) => c.copy(day = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        print(params)
        run(params)
      case _ => sys.exit(1)

    }
  }


  def run(params: Param): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    // update日期和full日期
    val day = params.day
    val pday = DateUtils.minusDays(params.day, 1)

    spark.udf.register("map_concat_2", (ms1: Map[String, Seq[String]], ms2: Map[String, Seq[String]]) => {
      if (ms2 == null) {
        ms1
      } else if (ms1 == null) {
        ms2
      } else {
        ms1 ++ ms2
      }
    })

    sql(
      spark,
      s"""
         |SELECT device
         |     , max(update_time) as update_time
         |     , map_concat_2(first(confidence_full, true), first(confidence_incr, true)) as confidence
         |     , nvl(first(tag_list_incr, true), first(tag_list_full, true)) as tag_list
         |     , nvl(first(catelist_incr, true), first(catelist_full, true)) as catelist
         |     , map_concat_2(first(tags_incr, true), first(tags_full, true)) as tags
         |FROM (
         |      SELECT device
         |           , day as update_time
         |           , confidence as confidence_incr, null as confidence_full
         |           , tag_list as tag_list_incr, null as tag_list_full
         |           , catelist as catelist_incr, null as catelist_full
         |           , tags as tags_incr, null as tags_full
         |      FROM ${PropUtils.HIVE_TABLE_PROFILE_ALL_TAGS_INFO_DI_V2}
         |      WHERE day = '$day'
         |
         |      UNION ALL
         |
         |      SELECT device
         |           , update_time
         |           , null as confidence_incr, confidence as confidence_full
         |           , null as tag_list_incr, tag_list as tag_list_full
         |           , null as catelist_incr, catelist as catelist_full
         |           , null as tags_incr, tags as tags_full
         |      FROM ${PropUtils.HIVE_TABLE_PROFILE_TAGS_INFO_FULL_V2}
         |      WHERE day = '$pday'
         |     )
         |GROUP BY device
         |""".stripMargin).createOrReplaceTempView("t1")

    sql(
      spark,
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_PROFILE_TAGS_INFO_FULL_V2} partition(day = '$day')
         |select device
         |     , update_time
         |     , confidence
         |     , tag_list
         |     , catelist
         |     , tags
         |from t1
         |""".stripMargin)

    spark.close()
  }

  def sql(spark: SparkSession, query: String): DataFrame = {
    println(
      s"""
         |<<<<<<
         |$query
         |>>>>>>
        """.stripMargin)
    spark.sql(query)
  }

}
