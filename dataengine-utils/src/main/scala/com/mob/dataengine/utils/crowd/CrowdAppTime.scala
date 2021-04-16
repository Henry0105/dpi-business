package com.mob.dataengine.utils.crowd

import java.time.YearMonth

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable, targetTable}
import com.mob.dataengine.commons.enums.AppStatus
import com.mob.dataengine.commons.helper.{BitDate, DateUtils}
import com.mob.dataengine.commons.traits.Logging
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

@author("xlmeng")
@createTime("20201026")
@sourceTable("dm_mobdi_master.master_reserved_new, dm_mobdi_master.device_active_applist," +
  "dm_sdk_mapping.pkg_name_mapping")
@targetTable("dm_dataengine_tags.device_app_time_status_full")
object CrowdAppTime {

  val resTable = "res_table"

  def main(args: Array[String]): Unit = {
    val defaultParams = CrowdAppTimeParam()
    val projectName = s"TagsGenerator[${DateUtils.getCurrentDay()}]"
    val parser = new OptionParser[CrowdAppTimeParam](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .required()
        .text(s"tag更新时间")
        .action((x, c) => c.copy(day = x))
      opt[Boolean]('f', "full")
        .optional()
        .text(s"是否全量表")
        .action((x, c) => c.copy(full = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(p) =>
        println(p)
        val spark: SparkSession = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()
        CrowdAppTime(spark, p).run()
        spark.close()
      case _ =>
        println(s"参数有误:${args.mkString(",")}")
        sys.exit(1)
    }
  }


}

case class CrowdAppTime(@transient spark: SparkSession, p: CrowdAppTimeParam) extends Logging {

  val day: String = p.day

  import CrowdAppTime._

  def run(): Unit = {
    if (p.full) {
      genFullTable()
    } else {
      genIncrTable()
    }
  }

  def genFullTable(): Unit = {
    val thisDayOfLastYear = DateUtils.minusYears(day, 1)
    val (y, m, _) = DateUtils.getYearMonthDay(day)
    spark.udf.register("construct_bitDates", new BitDate.ConstructBitDates(y, m))
    sql(
      s"""
         |SELECT device, pkg
         |     , construct_bitDates(status, day) as status_bitDates
         |     , max(day) as upper_time
         |FROM (
         |      SELECT device, pkg, refine_final_flag as status, day -- -1 卸载 -1 0 1 2 在装 1 新安装
         |      FROM ${PropUtils.HIVE_TABLE_MASTER_RESERVED_NEW}
         |      WHERE day between '$thisDayOfLastYear' and '$day'
         |      UNION ALL
         |      SELECT device, pkg, 3 as status, day
         |      FROM ${PropUtils.HIVE_TABLE_DEVICE_ACTIVE_APPLIST}
         |      WHERE day between '$thisDayOfLastYear' and '$day'
         |     ) AS a
         |GROUP BY device, pkg
         |""".stripMargin).createOrReplaceTempView("full_t1")

    sql(
      s"""
         |SELECT device, b.pkg, c.name as app_name
         |     , status_bitDates[${AppStatus.installed.id}] as installed
         |     , status_bitDates[${AppStatus.active.id}] as active
         |     , status_bitDates[${AppStatus.uninstall.id}] as uninstall
         |     , status_bitDates[${AppStatus.newInstall.id}] as new_install
         |     , upper_time
         |FROM full_t1 b
         |LEFT JOIN ${PropUtils.HIVE_TABLE_DM_PKG_NAME_MAPPING} c
         |ON b.pkg = c.pkg
         |""".stripMargin).createOrReplaceTempView(resTable)

    persist2Hive()
  }

  def genIncrTable(): Unit = {
    val (_, _, d) = DateUtils.getYearMonthDay(day)
    val dayInt = BitDate.calDayInt(d)

    spark.udf.register("add_to_bitDateArr", BitDate.addToBitDateArr(0, dayInt) _)

    // 每日增量计算
    sql(
      s"""
         |SELECT device, pkg
         |     , max(installed_flag) as installed_flag
         |     , max(active_flag) as active_flag
         |     , max(uninstall_flag) as uninstall_flag
         |     , max(new_install_flag) as new_install_flag
         |FROM (
         |      SELECT device, pkg
         |           , case when refine_final_flag is not null then 1
         |                                    else 0 end as installed_flag
         |           , case refine_final_flag when 3 then 1 else 0 end as active_flag
         |           , case refine_final_flag when -1 then 1 else 0 end as uninstall_flag
         |           , case refine_final_flag when 1 then 1 else 0 end as new_install_flag
         |      FROM (
         |            SELECT device, pkg, refine_final_flag -- -1 卸载 0 在装 1 新安装
         |            FROM ${PropUtils.HIVE_TABLE_MASTER_RESERVED_NEW}
         |            WHERE day = '$day'
         |
         |            UNION ALL
         |
         |            SELECT device, pkg, 3 as refine_final_flag
         |            FROM ${PropUtils.HIVE_TABLE_DEVICE_ACTIVE_APPLIST}
         |            WHERE day = '$day'
         |           ) AS a
         |     ) AS b
         |GROUP BY device, pkg
         |""".stripMargin).createOrReplaceTempView("incr_t1")

    // join上app_name
    sql(
      s"""
         |SELECT device, c.pkg, d.name as app_name
         |     , installed_flag, active_flag, uninstall_flag, new_install_flag
         |FROM incr_t1 c
         |LEFT JOIN ${PropUtils.HIVE_TABLE_DM_PKG_NAME_MAPPING} d
         |ON c.pkg = d.pkg
         |""".stripMargin).createOrReplaceTempView("incr_t2")

    val pday = DateUtils.minusDays(day, 1)
    // 获取昨日的全量表
    getYesterdayTable(pday).createOrReplaceTempView("full_yesterday")
    // 增量更新
    sql(
      s"""
         |SELECT device, pkg, first(app_name, true) as app_name
         |     , first(installed, true) as installed
         |     , max(installed_flag) as installed_flag
         |     , first(active, true) as active
         |     , max(active_flag) as active_flag
         |     , first(uninstall, true) as uninstall
         |     , max(uninstall_flag) as uninstall_flag
         |     , first(new_install, true) as new_install
         |     , max(new_install_flag) as new_install_flag
         |     , max(upper_time) as upper_time
         |FROM (
         |      SELECT device, pkg, app_name
         |           , null as installed, installed_flag
         |           , null as active, active_flag
         |           , null as uninstall, uninstall_flag
         |           , null as new_install, new_install_flag
         |           , '$day'  as upper_time
         |      FROM incr_t2
         |
         |      UNION ALL
         |
         |      SELECT device, pkg, app_name
         |           , installed, 0 as installed_flag
         |           , active, 0 as active_flag
         |           , uninstall, 0 as uninstall_flag
         |           , new_install, 0 as new_install_flag
         |           , upper_time
         |      FROM full_yesterday
         |     ) AS e
         |GROUP BY device, pkg
         |""".stripMargin).createOrReplaceTempView("incr_t3")

    sql(
      s"""
         |SELECT device, pkg, app_name
         |     , add_to_bitDateArr(installed, installed_flag) as installed
         |     , add_to_bitDateArr(active, active_flag) as active
         |     , add_to_bitDateArr(uninstall, uninstall_flag) as uninstall
         |     , add_to_bitDateArr(new_install, new_install_flag) as new_install
         |     , upper_time
         |FROM incr_t3
         |""".stripMargin).createOrReplaceTempView(resTable)

    persist2Hive()
  }

  def persist2Hive(): Unit = {
    sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll} PARTITION (day = '$day')
         |SELECT device, pkg, app_name
         |     , installed, active, uninstall, new_install
         |     , upper_time
         |FROM $resTable
         |""".stripMargin)
  }


  private def getYesterdayTable(pday: String): DataFrame = {
    val df = sql(
      s"""
         |SELECT device, pkg, app_name
         |     , installed, 0 as installed_flag
         |     , active, 0 as active_flag
         |     , uninstall, 0 as uninstall_flag
         |     , new_install, 0 as new_install_flag
         |     , upper_time
         |FROM ${PropUtils.HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll}
         |WHERE day = '$pday'
         |""".stripMargin)
    replaceAndRemoveOldMonth(df, pday)
  }

  private def replaceAndRemoveOldMonth(df: DataFrame, pday: String): DataFrame = {
    val date = DateUtils.getDate(pday)
    val (pY, pM, _) = DateUtils.getYearMonthDay(pday)
    if (date.compareTo(YearMonth.of(pY, pM).atEndOfMonth()) != 0) {
      // 不是月末,不替换
      df
    } else {
      println("删除上个月月末数据")
      // 是月末,需要用当前月替换掉旧月份
      import org.apache.spark.sql.functions._
      import spark.implicits._
      val bound = DateUtils.minusYears(pday, 1)
      val replaceAndRemoveOldMonthUDF = udf(BitDate.replaceAndRemoveOldMonth(pM) _)
      val updateUpperTimeUDF = udf(BitDate.updateUpperTime(bound) _)

      df.withColumn("installed", replaceAndRemoveOldMonthUDF($"installed"))
        .withColumn("active", replaceAndRemoveOldMonthUDF($"active"))
        .withColumn("uninstall", replaceAndRemoveOldMonthUDF($"uninstall"))
        .withColumn("new_install", replaceAndRemoveOldMonthUDF($"new_install"))
        .withColumn("upper_time", updateUpperTimeUDF($"upper_time"))
        .where($"upper_time".isNotNull)
    }
  }

}
