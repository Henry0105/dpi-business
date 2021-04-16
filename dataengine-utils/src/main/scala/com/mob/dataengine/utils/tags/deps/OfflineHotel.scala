package com.mob.dataengine.utils.tags.deps
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, Row}

case class OfflineHotel(cxt: Context) extends AbstractDataset(cxt) {
  override val datasetId: String = s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_6_30"

  override def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day)

    val sep = "\u0001"
    val features = getTagIdMapping().keys.map(s => s"'$s'")
    val tagId2ValueMap = s"map(${getTagIdMapping().map { case (k, v) => s"'$k', '$v'" }.mkString(",")})"

    sql(
      s"""
         |select device, concat_ws('\u0002', concat_ws('$sep', collect_list(tag_id)),
         |  concat_ws('$sep', collect_list(cnt))), day
         |from (
         |  select device, $tagId2ValueMap[feature] tag_id, cnt, day
         |  from ${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}
         |  where day='$day' and timewindow = 30 and flag = 6 and ${sampleClause()}
         |    and feature in (${features.mkString(",")})
         |) as a
         |group by device, day
       """.stripMargin)
      .toDF("device", "cateid_40", "day")
    .filter("length(cateid_40)>1")
  }

  def getTagIdMapping(): Map[String, String] = {
    Map(
      "hotel_style" -> "4456_1000", "brand" -> "4457_1000", "total" -> "4458_1000",
      "rank_star" -> "4459_1000", "facilities" -> "4460_1000", "score_type" -> "4461_1000",
      "price_level" -> "4462_1000", "location" -> "4463_1000"
    )
  }
}
