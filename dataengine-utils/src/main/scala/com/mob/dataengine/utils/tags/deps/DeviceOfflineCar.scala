package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, Row}

case class DeviceOfflineCar(cxt: Context) extends AbstractDataset(cxt) {
  override val datasetId: String = s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_3_30"

  override def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day)

    val sep = "\u0001"

    val features = getTagIdMapping().keys.map(s => s"'$s'")
    val tagId2ValueMap = s"map(${getTagIdMapping().map{ case (k, v) => s"'$k', '$v'"}.mkString(",")})"

    sql(
      s"""
         |select device, concat_ws('\u0002', concat_ws('$sep', collect_list(tag_id)),
         |  concat_ws('$sep', collect_list(cnt))), day
         |from (
         |  select device, $tagId2ValueMap[feature] tag_id, cnt, day
         |  from ${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}
         |  where day='$day' and timewindow = 30 and flag = 3 and ${sampleClause()}
         |    and feature in (${features.mkString(",")})
         |) as tmp
         |group by device, day
       """.stripMargin)
      .toDF("device", "cateid_618", "day")
      .filter("length(cateid_618)>1")
  }

  def getTagIdMapping(): Map[String, String] = {
    Map(
      "brand" -> "4489_1000", "zone" -> "4490_1000", "price" -> "4491_1000", "type" -> "4492_1000"
    )
  }
}
