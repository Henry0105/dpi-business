package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

case class FinanceAction(cxt: Context) extends AbstractDataset(cxt) {
  override def timeCol: Column = dataset.col("processtime")
  @transient private[this] val logger = LoggerFactory.getLogger(getClass)

  val datasetId: String = ""

  override lazy val lastTsOpt = Option(Seq(
    cxt.tableStateManager.lastTableStates.getOrElse(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_3_30", ""),
    cxt.tableStateManager.lastTableStates.getOrElse(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_30", ""),
    cxt.tableStateManager.lastTableStates.getOrElse(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_7", "")
  ).min)

  def fieldsToOneFuncName: String = {
    val func = s"FieldsToOne_${RandomStringUtils.randomAlphanumeric(5)}"
    sql(s"""create temporary function $func as 'com.youzu.mob.java.udf.FieldsToOne'""")
    logger.info(s"temporary function $func")
    func
  }

  def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_7")
    val installDay = cxt.tablePartitions(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_30")
    val activeDay = cxt.tablePartitions(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_3_30")

    cxt.update(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_7", day)
    cxt.update(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_30", installDay)
    cxt.update(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_3_30", activeDay)

    val financialInstallProfile = "financial_install_profile"
    onlineProfile2FinanceAdapter(Map("fin07_0_7" -> "total", "fin06_0_7" -> "finaces", "fin05_0_7" -> "securities",
      "fin03_0_7" -> "investment", "fin02_0_7" -> "bank", "fin01_0_7" -> "borrowing", "fin04_0_7" -> "insurance"),
      financialInstallProfile, day, 0, 7)

    val financialActiveMonthProfile = "financial_active_month_profile"
    onlineProfile2FinanceAdapter(Map("fin07_3_30" -> "total", "fin06_3_30" -> "finaces", "fin05_3_30" -> "securities",
      "fin03_3_30" -> "investment", "fin02_3_30" -> "bank", "fin01_3_30" -> "borrowing", "fin04_3_30" -> "insurance"),
      financialActiveMonthProfile, day, 3, 30)

    val financialActive3monthProfile = "financial_active_3month_profile"
    onlineProfile2FinanceAdapter(Map("fin07_3_90" -> "total", "fin06_3_90" -> "finaces", "fin05_3_90" -> "securities",
      "fin03_3_90" -> "investment", "fin02_3_90" -> "bank", "fin01_3_90" -> "borrowing", "fin04_3_90" -> "insurance"),
      financialActive3monthProfile, day, 3, 90)

    val financialActiveWeekProfile = "financial_active_week_profile"
    onlineProfile2FinanceAdapter(Map("fin07_3_7" -> "total", "fin06_3_7" -> "finaces", "fin05_3_7" -> "securities",
      "fin03_3_7" -> "investment", "fin02_3_7" -> "bank", "fin01_3_7" -> "borrowing", "fin04_3_7" -> "insurance"),
      financialActiveWeekProfile, day, 3, 7)

    val financialSlopeWeekProfile = "financial_slope_week_profile"
    onlineProfile2FinanceAdapter(Map("fin07_6_7" -> "total_slope", "fin06_6_7" -> "finaces_slope",
      "fin05_6_7" -> "securities_slope", "fin03_6_7" -> "investment_slope", "fin02_6_7" -> "bank_slope",
      "fin01_6_7" -> "borrowing_slope", "fin04_6_7" -> "insurance_slope"),
      financialSlopeWeekProfile, day, 6, 7)

    sql(
      s"""
         |SELECT coalesce(cc.device, dd.device) AS device,
         |       coalesce(cc.day, dd.day, f_day, g_day, h_day, i_day) AS finance_time,
         |       $fieldsToOneFuncName (
         |        'F1001|F1002|F1003|F1004|F1005|F1006|F1007|F1008|F1009|F1010|F1011|F0012|F1013|F1014|F1015|
         |        F1016|F1017|F1018|F1019|F1020|F1021|F1022|F1023|F1024|F1025|F1026|F1027|F1028|F1029|F1030|F1031|
         |        F1032|F1033|F1034|F1035|F1036|F1037|F1038|F1039',
         |         borrowing1, bank2, investment3, finaces4, securities5, insurance6, total7, insurance8, borrowing9,
         |         bank10,
         |         investment11, securities12, finaces13, total14, insurance15, borrowing16, bank17, investment18,
         |         securities19, finaces20,
         |         total21, insurance22, borrowing23, bank24, investment25, securities26, finaces27, total28,
         |         insurance_slope29,
         |         borrowing_slope30, bank_slope31, investment_slope32, securities_slope33, finaces_slope34,
         |         total_slope35,
         |         coalesce(f_cnt, ''), coalesce(g_cnt, ''), coalesce(h_cnt, ''), coalesce(i_cnt, '')
         |       ) AS finance_action,
         |       coalesce(cc.day, dd.day, f_day, g_day, h_day, i_day) AS processtime
         |FROM
         |  (SELECT coalesce(bb.device, d.device) AS device,
         |          borrowing1,
         |          bank2,
         |          investment3,
         |          finaces4,
         |          securities5,
         |          insurance6,
         |          total7,
         |          insurance8,
         |          borrowing9,
         |          bank10,
         |          investment11,
         |          securities12,
         |          finaces13,
         |          total14,
         |          insurance15,
         |          borrowing16,
         |          bank17,
         |          investment18,
         |          securities19,
         |          finaces20,
         |          total21,
         |          insurance22,
         |          borrowing23,
         |          bank24,
         |          investment25,
         |          securities26,
         |          finaces27,
         |          total28,
         |          coalesce(bb.day, d.day) as day
         |   FROM
         |     (SELECT coalesce(aa.device, c.device) AS device,
         |             borrowing1,
         |             bank2,
         |             investment3,
         |             finaces4,
         |             securities5,
         |             insurance6,
         |             total7,
         |             insurance8,
         |             borrowing9,
         |             bank10,
         |             investment11,
         |             securities12,
         |             finaces13,
         |             total14,
         |             insurance15,
         |             borrowing16,
         |             bank17,
         |             investment18,
         |             securities19,
         |             finaces20,
         |             total21,
         |             coalesce(aa.day, c.day) as day
         |      FROM
         |        (SELECT coalesce(a.device, b.device) AS device,
         |                borrowing1,
         |                bank2,
         |                investment3,
         |                finaces4,
         |                securities5,
         |                insurance6,
         |                total7,
         |                insurance8,
         |                borrowing9,
         |                bank10,
         |                investment11,
         |                securities12,
         |                finaces13,
         |                total14,
         |                coalesce(a.day, b.day) as day
         |         FROM
         |           (SELECT device,
         |                   borrowing AS borrowing1,
         |                   bank AS bank2,
         |                   investment AS investment3,
         |                   finaces AS finaces4,
         |                   securities AS securities5,
         |                   insurance AS insurance6,
         |                   total AS total7,
         |                   day
         |            FROM $financialInstallProfile
         |           )a
         |         FULL OUTER JOIN
         |           (SELECT device,
         |                   insurance AS insurance8,
         |                   borrowing AS borrowing9,
         |                   bank AS bank10,
         |                   investment AS investment11,
         |                   securities AS securities12,
         |                   finaces AS finaces13,
         |                   total AS total14,
         |                   day
         |            FROM $financialActiveMonthProfile
         |           )b ON a.device=b.device
         |      ) aa
         |      FULL OUTER JOIN
         |      (SELECT device,
         |                insurance AS insurance15,
         |                borrowing AS borrowing16,
         |                bank AS bank17,
         |                investment AS investment18,
         |                securities AS securities19,
         |                finaces AS finaces20,
         |                total AS total21,
         |                day
         |         FROM $financialActive3monthProfile
         |       ) c ON aa.device=c.device
         |   ) bb
         |   FULL OUTER JOIN
         |   (SELECT device,
         |             insurance AS insurance22,
         |             borrowing AS borrowing23,
         |             bank AS bank24,
         |             investment AS investment25,
         |             securities AS securities26,
         |             finaces AS finaces27,
         |             total AS total28,
         |             day
         |      FROM $financialActiveWeekProfile
         |   )d ON bb.device=d.device
         |) cc
         |FULL OUTER JOIN
         |(
         |  select coalesce(e.device, online_tw_agg.device) device, insurance_slope29, borrowing_slope30,
         |    bank_slope31, investment_slope32, securities_slope33, finaces_slope34, total_slope35, day,
         |    f_cnt, f_day, g_cnt, g_day, h_cnt, h_day, i_cnt, i_day
         |  from (
         |    SELECT device,
         |      insurance_slope AS insurance_slope29,
         |      borrowing_slope AS borrowing_slope30,
         |      bank_slope AS bank_slope31,
         |      investment_slope AS investment_slope32,
         |      securities_slope AS securities_slope33,
         |      finaces_slope AS finaces_slope34,
         |      total_slope AS total_slope35,
         |      day
         |    FROM $financialSlopeWeekProfile
         |  ) e
         |  FULL OUTER JOIN
         |  (
         |    select device, max(f_cnt) as f_cnt, max(f_day) as f_day, max(g_cnt) as g_cnt, max(g_day) as g_day,
         |      max(h_cnt) as h_cnt, max(h_day) as h_day, max(i_cnt) as i_cnt, max(i_day) as i_day
         |    from (
         |      SELECT device,
         |        if(feature='fin06_0_30', cnt, null) as f_cnt, if(feature='fin06_0_30', day, null) as f_day,
         |        if(feature='fin01_0_30', cnt, null) as g_cnt, if(feature='fin01_0_30', day, null) as g_day,
         |        if(feature='505_0_30', cnt, null) as h_cnt, if(feature='505_0_30', day, null) as h_day,
         |        null as i_cnt, null as i_day
         |      FROM ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2} as f
         |      WHERE flag=0 AND timewindow=30 AND feature in('fin06_0_30', 'fin01_0_30', '505_0_30')
         |        AND day = '$installDay'
         |      and ${sampleClause()}
         |
         |      union all
         |
         |      SELECT device, null as f_cnt, null as f_day, null as g_cnt, null as g_day,
         |        null as h_cnt, null as h_day, cnt as i_cnt, day as i_day
         |      FROM ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2} as i
         |      WHERE flag=3 AND timewindow=30 AND feature = '505_3_30' AND day = '$installDay' and ${sampleClause()}
         |    ) as online_tw
         |    group by device
         |  ) as online_tw_agg
         |  on e.device = online_tw_agg.device
         |) as dd
         |on cc.device = dd.device
       """.stripMargin)
  }
}
