package com.mob.dataengine.utils

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.DateFormatUtils
import java.time.{LocalDate, YearMonth}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession

/**
 * @author juntao zhang
 */
object DateUtils {
  def format(time: Long): String = {
    DateFormatUtils.format(time, "yyyyMMdd")
  }

  def currentDay(): String = {
    format(System.currentTimeMillis())
  }

  def currentTime(): String = {
    DateFormatUtils.format(System.currentTimeMillis(), "yyyyMMddHHmm")
  }

  /**
   * 20180044, 20180045, 获取这个周的最后一天,如果为53周,则为当年的最后一天, 如果为00(20190000)则为前一年的最后一天
   */
  def convWeek2Day(date: String): String = {
    val year = date.substring(0, 4).toInt
    val week = date.substring(6, 8).toInt
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    if (week > 52) {
      LocalDate.now().withYear(year).`with`(java.time.temporal.TemporalAdjusters.lastDayOfYear).format(dtf)
    } else if (week == 0) {
      LocalDate.now().withYear(year)
        .minusYears(1).`with`(java.time.temporal.TemporalAdjusters.lastDayOfYear).format(dtf)
    } else {
      LocalDate.ofYearDay(year, week * 7).format(dtf)
    }
  }

  /**
   * 20180900, 20181000 获取当月的最后一天
   */
  def convMonth2Day(month: String): String = {
    val ym = YearMonth.parse(month.substring(0, 6), DateTimeFormatter.ofPattern("yyyyMM"))
    ym.atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
  }

  /**
   * 获取以当前时间为基准，前几天或者后几天的时间戳
   */
  def getDayTimeStamp(day: String, beforeDay: Int): String = {
    val ts = beforeDay * 86400000
    val dateTime = new SimpleDateFormat("yyyyMMdd").parse(day).getTime
    (dateTime + ts).toString.substring(0, 10)
  }

  /**
   * 获取以当前时间为基准，前几天或者后几天的时间
   */
  def getDayBefore(day: String, span: Int): String = {
    val ts = span * 86400000
    val dateTime = new SimpleDateFormat("yyyyMMdd").parse(day).getTime
    format(dateTime + ts)
  }
}
