package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.DateUtils
import org.apache.spark.sql.DataFrame

case class CateringStyle(cxt: Context) extends AbstractDataset(cxt) {
  val datasetId: String = ""
  val monthlyDay = cxt.tablePartitions(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_30")
  val weeklyDay = cxt.tablePartitions(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_7")

  override lazy val lastTsOpt = Option(Seq(
    cxt.tableStateManager.lastTableStates.getOrElse(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_30", ""),
    cxt.tableStateManager.lastTableStates.getOrElse(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_7", "")
  ).min)

  override def _dataset(): DataFrame = {
    cxt.update(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_30", monthlyDay)
    cxt.update(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_7", weeklyDay)

    val sep = "\u0001"
    val fields = getTagIdMapping().keys.map(s => s.substring(0, s.lastIndexOf("_")))
    val tagIdMappingBC = cxt.spark.sparkContext.broadcast(getTagIdMapping())
    val weeklyFields = fields.map(s => s"${s}_weekly")
    val monthlyFields = fields.map(s => s"${s}_monthly")
    val allFields = weeklyFields ++ monthlyFields

    val tagId2ValueMap = s"map(${getTagIdMapping().map{ case (k, v) => s"'$k', '$v'"}.mkString(",")})"

    // concat_ws('\u0002', concat_ws('\u0001', map[f1], map[f2]), concat_ws('\u0001', f1, f2))
    val collapseFieldsSQL =
      s"""
         |concat_ws('\u0002',
         |  concat_ws('$sep', ${allFields.map(f => s"$tagId2ValueMap['$f']").mkString(",")}),
         |  concat_ws('$sep', ${allFields.mkString(",")})
         |)
       """.stripMargin

    val cateringLbsLabelWeekly = "catering_lbs_label_weekly"
    onlineProfile2FinanceAdapter(Map("brand" -> "catering_dinein_brand_detail",
      "category1" -> "catering_dinein_tyle1_detail", "category2" -> "catering_dinein_tyle2_detail",
      "taste" -> "catering_dinein_taste_detail"),
      cateringLbsLabelWeekly, weeklyDay, 1, 7, false)

    val cateringLbsLabelMonthly = "catering_lbs_label_monthly"
    onlineProfile2FinanceAdapter(Map("brand" -> "catering_dinein_brand_detail",
      "category1" -> "catering_dinein_tyle1_detail", "category2" -> "catering_dinein_tyle2_detail",
      "taste" -> "catering_dinein_taste_detail"),
      cateringLbsLabelMonthly, monthlyDay, 1, 30, false)

    sql(
      s"""
        |select device, compact_tags($collapseFieldsSQL), day
        |from (
        |  select coalesce(weekly.device, monthly.device) device, ${weeklyFields.mkString(",")},
        |    ${monthlyFields.mkString(",")},
        |    case
        |      when weekly.day is null then monthly.day
        |      when monthly.day is null then weekly.day
        |      else if(monthly.day > weekly.day, monthly.day, weekly.day)
        |    end as day
        |  from (
        |    select device, ${fields.zip(weeklyFields).map{ case (f, w) => s"$f as $w" }.mkString(",")}, day
        |    from $cateringLbsLabelWeekly
        |  ) as weekly
        |  full outer join (
        |    select device, ${fields.zip(monthlyFields).map { case (f, w) => s"$f as $w" }.mkString(",")}, day
        |    from $cateringLbsLabelMonthly
        |  ) as monthly
        |  on weekly.device = monthly.device
        |) as tmp
      """.stripMargin)
      .toDF("device", "cateid_34", "day")
      .filter("length(cateid_34)>1")
  }

  def getTagIdMapping(): Map[String, String] = {
    Map(
      "catering_dinein_brand_detail_monthly" -> "4471_1000",
      "catering_dinein_tyle1_detail_monthly" -> "4473_1000",
      "catering_dinein_tyle2_detail_monthly" -> "4475_1000",
      "catering_dinein_taste_detail_monthly" -> "4477_1000",
      "catering_dinein_brand_detail_weekly" -> "4470_1000",
      "catering_dinein_tyle1_detail_weekly" -> "4472_1000",
      "catering_dinein_tyle2_detail_weekly" -> "4474_1000",
      "catering_dinein_taste_detail_weekly" -> "4476_1000"
    )
  }
}
