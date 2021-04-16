package com.mob.dataengine.utils.tags.deps
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

case class MultiloanFinance(cxt: Context) extends AbstractDataset(cxt) {
  override val datasetId: String = s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_90"

  override def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day)
    import cxt.spark.implicits._

    val sep = "\u0001"
    val tagId2ValueMap = s"map(${getTagIdMapping().map { case (k, v) => s"'$k', '$v'" }.mkString(",")})"
    val featureFilter = getTagIdMapping().keys.map(s => s"'$s'").mkString(",")

    // 715: "3034_1000", "3049_1000"
    // 716: "3050_1000", "3065_1000"
    // 717: "3066_1000", "3081_1000"
    // 718: "3082_1000", "3097_1000"
    // 719: "3098_1000", "3113_1000"
    // 720: "3114_1000", "3129_1000"

    sql(
      s"""
        |select device,
        |  concat_ws('\u0002',
        |    concat_ws('$sep', collect_list(if(tag_id >= '3034_1000' and tag_id <= '3049_1000', tag_id, null))),
        |    concat_ws('$sep', collect_list(if(tag_id >= '3034_1000' and tag_id <= '3049_1000', cnt, null))))
        |  as cateid_715,
        |  concat_ws('\u0002',
        |    concat_ws('$sep', collect_list(if(tag_id >= '3050_1000' and tag_id <= '3065_1000', tag_id, null))),
        |    concat_ws('$sep', collect_list(if(tag_id >= '3050_1000' and tag_id <= '3065_1000', cnt, null))))
        |  as cateid_716,
        |  concat_ws('\u0002',
        |    concat_ws('$sep', collect_list(if(tag_id >= '3066_1000' and tag_id <= '3081_1000', tag_id, null))),
        |    concat_ws('$sep', collect_list(if(tag_id >= '3066_1000' and tag_id <= '3081_1000', cnt, null))))
        |  as cateid_717,
        |  concat_ws('\u0002',
        |    concat_ws('$sep', collect_list(if(tag_id >= '3082_1000' and tag_id <= '3097_1000', tag_id, null))),
        |    concat_ws('$sep', collect_list(if(tag_id >= '3082_1000' and tag_id <= '3097_1000', cnt, null))))
        |  as cateid_718,
        |  concat_ws('\u0002',
        |    concat_ws('$sep', collect_list(if(tag_id >= '3098_1000' and tag_id <= '3113_1000', tag_id, null))),
        |    concat_ws('$sep', collect_list(if(tag_id >= '3098_1000' and tag_id <= '3113_1000', cnt, null))))
        |  as cateid_719,
        |  concat_ws('\u0002',
        |    concat_ws('$sep', collect_list(if(tag_id >= '3114_1000' and tag_id <= '3129_1000', tag_id, null))),
        |    concat_ws('$sep', collect_list(if(tag_id >= '3114_1000' and tag_id <= '3129_1000', cnt, null))))
        |  as cateid_720,
        |  day
        |from (
        |  select device, $tagId2ValueMap[feature] tag_id, cast(cnt as string) cnt, day
        |  from ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}
        |  where day='$day' and flag in (0, 3, 4, 5) and timewindow in ('7', '14', '30', '90')
        |    and ${sampleClause()} and feature in ($featureFilter)
        |) as a
        |group by device, day
      """.stripMargin)
      .toDF("device", "cateid_715", "cateid_716", "cateid_717", "cateid_718", "cateid_719", "cateid_720", "day")
      .map{r =>
        val cols = Seq("cateid_715", "cateid_716", "cateid_717", "cateid_718", "cateid_719", "cateid_720")
        val values = cols.map(c => if (1 >= r.getAs[String](c).trim.length) null else r.getAs[String](c))

        (
          r.getAs[String]("device"),
          values(0), values(1), values(2), values(3), values(4), values(5),
          r.getAs[String]("day")
        )
      }.toDF("device", "cateid_715", "cateid_716", "cateid_717", "cateid_718", "cateid_719", "cateid_720", "day")
  }

  def getTagIdMapping(): Map[String, String] = {
    Map(
      "1_0_7" -> "3034_1000", "1_0_14" -> "3035_1000", "1_0_30" -> "3036_1000", "1_0_90" -> "3037_1000",
      "1_3_7" -> "3038_1000", "1_3_14" -> "3039_1000", "1_3_30" -> "3040_1000", "1_3_90" -> "3041_1000",
      "1_4_7" -> "3042_1000", "1_4_14" -> "3043_1000", "1_4_30" -> "3044_1000", "1_4_90" -> "3045_1000",
      "1_5_7" -> "3046_1000", "1_5_14" -> "3047_1000", "1_5_30" -> "3048_1000", "1_5_90" -> "3049_1000",
      "2_0_7" -> "3050_1000", "2_0_14" -> "3051_1000", "2_0_30" -> "3052_1000", "2_0_90" -> "3053_1000",
      "2_3_7" -> "3054_1000", "2_3_14" -> "3055_1000", "2_3_30" -> "3056_1000", "2_3_90" -> "3057_1000",
      "2_4_7" -> "3058_1000", "2_4_14" -> "3059_1000", "2_4_30" -> "3060_1000", "2_4_90" -> "3061_1000",
      "2_5_7" -> "3062_1000", "2_5_14" -> "3063_1000", "2_5_30" -> "3064_1000", "2_5_90" -> "3065_1000",
      "3_0_7" -> "3066_1000", "3_0_14" -> "3067_1000", "3_0_30" -> "3068_1000", "3_0_90" -> "3069_1000",
      "3_3_7" -> "3070_1000", "3_3_14" -> "3071_1000", "3_3_30" -> "3072_1000", "3_3_90" -> "3073_1000",
      "3_4_7" -> "3074_1000", "3_4_14" -> "3075_1000", "3_4_30" -> "3076_1000", "3_4_90" -> "3077_1000",
      "3_5_7" -> "3078_1000", "3_5_14" -> "3079_1000", "3_5_30" -> "3080_1000", "3_5_90" -> "3081_1000",
      "4_0_7" -> "3082_1000", "4_0_14" -> "3083_1000", "4_0_30" -> "3084_1000", "4_0_90" -> "3085_1000",
      "4_3_7" -> "3086_1000", "4_3_14" -> "3087_1000", "4_3_30" -> "3088_1000", "4_3_90" -> "3089_1000",
      "4_4_7" -> "3090_1000", "4_4_14" -> "3091_1000", "4_4_30" -> "3092_1000", "4_4_90" -> "3093_1000",
      "4_5_7" -> "3094_1000", "4_5_14" -> "3095_1000", "4_5_30" -> "3096_1000", "4_5_90" -> "3097_1000",
      "5_0_7" -> "3098_1000", "5_0_14" -> "3099_1000", "5_0_30" -> "3100_1000", "5_0_90" -> "3101_1000",
      "5_3_7" -> "3102_1000", "5_3_14" -> "3103_1000", "5_3_30" -> "3104_1000", "5_3_90" -> "3105_1000",
      "5_4_7" -> "3106_1000", "5_4_14" -> "3107_1000", "5_4_30" -> "3108_1000", "5_4_90" -> "3109_1000",
      "5_5_7" -> "3110_1000", "5_5_14" -> "3111_1000", "5_5_30" -> "3112_1000", "5_5_90" -> "3113_1000",
      "6_0_7" -> "3114_1000", "6_0_14" -> "3115_1000", "6_0_30" -> "3116_1000", "6_0_90" -> "3117_1000",
      "6_3_7" -> "3118_1000", "6_3_14" -> "3119_1000", "6_3_30" -> "3120_1000", "6_3_90" -> "3121_1000",
      "6_4_7" -> "3122_1000", "6_4_14" -> "3123_1000", "6_4_30" -> "3124_1000", "6_4_90" -> "3125_1000",
      "6_5_7" -> "3126_1000", "6_5_14" -> "3127_1000", "6_5_30" -> "3128_1000", "6_5_90" -> "3129_1000"
    )
  }
}
