package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame

case class OnlineGameActive(cxt: Context) extends AbstractDataset(cxt) {
  override val datasetId: String = s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_2_30"

  override def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day)

    import cxt.spark.implicits._

    val features = getTagIdMapping().keys.map(s => s"'$s'")
    val tagId2ValueMap = s"map(${getTagIdMapping().map{ case (k, v) => s"'$k', '$v'"}.mkString(",")})"

    val sep = "\u0001"

    sql(
      s"""
         |select device, concat_ws('\u0002', concat_ws('$sep', collect_list(tag_id)),
         |  concat_ws('$sep', collect_list(cnt))), day
         |from (
         |  select device, $tagId2ValueMap[feature] tag_id, cnt, day
         |  from ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}
         |  where day='$day' and timewindow = 30 and flag = 2 and ${sampleClause()}
         |    and length(feature) = 12 and substring(feature, 1, 4) = '7019'
         |) as a
         |group by device, day
       """.stripMargin)
      .toDF("device", "cateid_12", "day")
      .filter("length(cateid_12)>1")
  }

  def getTagIdMapping(): Map[String, String] = {
    Map(
      "7019101_2_30" -> "2797_1000", "7019102_2_30" -> "2798_1000", "7019103_2_30" -> "2799_1000",
      "7019104_2_30" -> "2800_1000", "7019105_2_30" -> "2801_1000", "7019106_2_30" -> "2802_1000",
      "7019107_2_30" -> "2803_1000", "7019108_2_30" -> "2804_1000", "7019109_2_30" -> "2805_1000",
      "7019110_2_30" -> "2806_1000", "7019111_2_30" -> "2807_1000", "7019112_2_30" -> "2808_1000",
      "7019113_2_30" -> "2809_1000", "7019114_2_30" -> "2810_1000", "7019115_2_30" -> "2811_1000",
      "7019116_2_30" -> "2812_1000", "7019117_2_30" -> "2813_1000", "7019118_2_30" -> "2814_1000",
      "7019119_2_30" -> "2815_1000", "7019120_2_30" -> "2816_1000", "7019121_2_30" -> "2817_1000",
      "7019122_2_30" -> "2818_1000", "7019123_2_30" -> "2819_1000", "7019124_2_30" -> "2820_1000",
      "7019125_2_30" -> "2821_1000", "7019126_2_30" -> "2822_1000", "7019127_2_30" -> "2823_1000",
      "7019128_2_30" -> "2824_1000", "7019129_2_30" -> "2825_1000", "7019130_2_30" -> "2826_1000",
      "7019132_2_30" -> "2827_1000", "7019133_2_30" -> "2828_1000", "7019134_2_30" -> "2829_1000",
      "7019135_2_30" -> "2830_1000", "7019136_2_30" -> "2831_1000", "7019137_2_30" -> "2832_1000",
      "7019138_2_30" -> "2833_1000", "7019139_2_30" -> "2834_1000", "7019140_2_30" -> "2835_1000",
      "7019141_2_30" -> "2836_1000"
    )
  }
}
