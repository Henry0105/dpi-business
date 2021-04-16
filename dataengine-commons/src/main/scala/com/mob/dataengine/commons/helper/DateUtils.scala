package com.mob.dataengine.commons.helper

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, Year, YearMonth}
import java.util.Locale

import com.mob.dataengine.commons.enums.PeriodEnum

/**
 * @author xlmeng
 */
object DateUtils {

  val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.CHINA)
  val lenOfWeek: Int = 7

  def getNowTT(): Timestamp = {
    Timestamp.from(Instant.now())
  }

  def getDate(day: String): LocalDate = {
    LocalDate.parse(day, fmt)
  }

  def getCurrentDay(): String = {
    fmt.format(LocalDate.now())
  }

  def plusDays(day: String, n: Int): String = {
    val date = LocalDate.parse(day, fmt)
    fmt.format(date.plusDays(n))
  }

  def minusDays(day: String, n: Int): String = {
    plusDays(day, -n)
  }

  def plusMonths(day: String, n: Int): String = {
    val date = LocalDate.parse(day, fmt)
    fmt.format(date.plusMonths(n))
  }

  def minusMonths(day: String, n: Int): String = {
    plusMonths(day, -n)
  }

  def plusYears(day: String, n: Int): String = {
    val date = LocalDate.parse(day, fmt)
    fmt.format(date.plusYears(n))
  }

  def minusYears(day: String, n: Int): String = {
    plusYears(day, -n)
  }

  /**
   * @return (年, 月, 日)
   */
  def getYearMonthDay(dateStr: String): (Int, Int, Int) = {
    val date = LocalDate.parse(dateStr, fmt)
    val year = date.getYear
    val month = date.getMonthValue
    val day = date.getDayOfMonth
    (year, month, day)
  }

  def getLastWeek(day: String): Seq[String] = {
    val date = LocalDate.parse(day, fmt)
    val lastSunday = date.minusDays(date.getDayOfWeek.getValue)
    (0 until 7).map(lastSunday.minusDays(_)).map(fmt.format(_))
  }

  def getLastMonth(day: String): Seq[String] = {
    val yearMonth = YearMonth.parse(day, fmt).minusMonths(1)
    val prefix = DateTimeFormatter.ofPattern("yyyyMM").format(yearMonth)
    (1 to yearMonth.lengthOfMonth()).map(i => prefix + f"$i%2d" replace(" ", "0"))
  }

  def getCalDay(day: String, period: String, periodDay: Int): String = {
    val date = LocalDate.parse(day, fmt)
    val value = PeriodEnum.withName(period.toLowerCase()) match {
      case PeriodEnum.day => date
      case PeriodEnum.week => date.minusDays(date.getDayOfWeek.getValue - periodDay)
      case PeriodEnum.month => YearMonth.parse(day, fmt).atDay(periodDay)
      case PeriodEnum.year => Year.parse(day, fmt).atDay(periodDay)
    }
    fmt.format(value)
  }

  def getAssignDate(day: String, period: String, periodDay: Int): String = {
    lazy val date = LocalDate.parse(day, fmt)
    val value = PeriodEnum.withName(period) match {
      case PeriodEnum.day => date
      case PeriodEnum.week =>
        val lastSunday = date.minusDays(date.getDayOfWeek.getValue)
        lastSunday.minusDays(lenOfWeek - periodDay)
      case PeriodEnum.month =>
        val lastMonth = YearMonth.parse(day, fmt).minusMonths(1)
        lastMonth.atDay(periodDay)
      case PeriodEnum.year =>
        val lastYear = Year.parse(day, fmt).minusYears(1)
        lastYear.atDay(periodDay)
    }
    fmt.format(value)
  }

}
