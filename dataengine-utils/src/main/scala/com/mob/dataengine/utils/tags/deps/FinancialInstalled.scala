package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * load_financial_installed_to_hbase.sh
 */
case class FinancialInstalled(cxt: Context) extends AbstractDataset(cxt) {
  @transient private[this] val logger = LoggerFactory.getLogger(getClass)
  val datasetId: String = s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_7_40"

  def _dataset(): DataFrame = {
    val fieldsToOneFuncName: String = {
      val func = s"FieldsToOne_${RandomStringUtils.randomAlphanumeric(5)}"
      sql(s"""create temporary function $func as 'com.youzu.mob.java.udf.FieldsToOne'""")
      logger.info(s"temporary function $func")
      func
    }

    // rp_device_financial_installed_profile timewindow_online_profile_10_1 取最小分区

    val installedCateTagPar = cxt.tablePartitions(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_7_40")

    cxt.update(s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_7_40", installedCateTagPar)

    val financialInstalledProfile = "financial_installed_profile"
    onlineProfile2FinanceAdapter(Map("fin07_7_40" -> "total", "fin06_7_40" -> "finaces", "fin05_7_40" -> "securities",
      "fin03_7_40" -> "investment", "fin02_7_40" -> "bank", "fin01_7_40" -> "borrowing", "fin04_7_40" -> "insurance",
      "505_7_40" -> "credit_card", "fin08_7_40" -> "percent"), financialInstalledProfile, installedCateTagPar, 7, 40)

    sql(
      s"""
         |SELECT CASE
         |        WHEN a.device IS NULL THEN b.device
         |        ELSE a.device
         |       END AS device,
         |       $fieldsToOneFuncName(
         |        'F0001|F0002|F0003|F0004|F0005|F0006|F0007|F0008|F0009|F0010',
         |        CASE
         |            WHEN a.borrowing IS NULL THEN ''
         |            ELSE a.borrowing
         |        END,
         |        CASE
         |           WHEN a.bank IS NULL THEN ''
         |           ELSE a.bank
         |        END,
         |        CASE
         |            WHEN a.investment IS NULL THEN ''
         |            ELSE a.investment
         |        END,
         |        CASE
         |            WHEN a.insurance IS NULL THEN ''
         |            ELSE a.insurance
         |        END,
         |        CASE
         |            WHEN a.securities IS NULL THEN ''
         |            ELSE a.securities
         |        END,
         |        CASE
         |            WHEN a.finaces IS NULL THEN ''
         |            ELSE a.finaces
         |        END,
         |        CASE
         |            WHEN a.credit_card IS NULL THEN ''
         |            ELSE a.credit_card
         |        END,
         |        CASE
         |            WHEN a.total IS NULL THEN ''
         |            ELSE a.total
         |        END,
         |        CASE
         |            WHEN a.percent IS NULL THEN ''
         |            ELSE a.percent
         |        END,
         |        CASE
         |            WHEN b.cnt IS NULL THEN ''
         |            ELSE b.cnt
         |        END
         |      ) AS installed_cate_tag,
         |      coalesce(b.day, a.day) as day
         |FROM (
         |  SELECT
         |    device,
         |    borrowing, bank, investment, insurance, securities, finaces, credit_card, total, percent,
         |    day
         |  FROM $financialInstalledProfile) as a
         |FULL JOIN
         |  (
         |    SELECT device, cnt, day
         |    FROM ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}
         |    WHERE flag=7
         |      AND timewindow=40
         |      AND day = '$installedCateTagPar'
         |      AND feature = '598_7_40'
         |      and ${sampleClause()}
         |  ) b ON a.device = b.device
      """.stripMargin)
  }

}
