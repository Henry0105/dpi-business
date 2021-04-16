package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

case class DmpCookPortrait(cxt: Context) extends AbstractDataset(cxt) {
  @transient private[this] val logger = LoggerFactory.getLogger(getClass)

  val datasetId: String = s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_1_30"

  def _dataset(): DataFrame = {
    val lbsDay = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, lbsDay)

    val cateringLbsLabelMonthly = "catering_lbs_label_monthly"
    onlineProfile2FinanceAdapter(Map("category1" -> "catering_dinein_tyle1_detail",
      "category2" -> "catering_dinein_tyle2_detail"),
      cateringLbsLabelMonthly, lbsDay, 1, 30, false)

    sql(
      s"""
         |select device,
         |    concat(
         |     'CA000|||CB000=',
         |      catering_dinein_tyle1_detail,
         |      '|||',
         |      catering_dinein_tyle2_detail
         |   ) as portrait_cook_tag,
         |   day
         |from $cateringLbsLabelMonthly
         |where catering_dinein_tyle1_detail is not null
         |  and catering_dinein_tyle2_detail is not null
      """.stripMargin)
  }

}
