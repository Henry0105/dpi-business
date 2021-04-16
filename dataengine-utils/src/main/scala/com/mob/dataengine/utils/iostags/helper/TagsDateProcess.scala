package com.mob.dataengine.utils.iostags.helper

import java.time.{LocalDate, Year, YearMonth}

import com.mob.dataengine.commons.enums.PeriodEnum
import com.mob.dataengine.commons.helper.DateUtils.fmt

/**
 * @author xlmeng
 */
object TagsDateProcess {

  /** 数据生成时间 hive表的数据分区 */
  def getDataDate(day: String, period: String, periodDay: Int): String = {
    val date = LocalDate.parse(day, fmt)
    val value = (PeriodEnum.withName(period.toLowerCase()): @unchecked) match {
      case PeriodEnum.day => date
      case PeriodEnum.week => date.minusDays(date.getDayOfWeek.getValue - periodDay)
      case PeriodEnum.month => YearMonth.parse(day, fmt).atDay(periodDay)
      case PeriodEnum.year => Year.parse(day, fmt).atDay(periodDay)
    }
    fmt.format(value)
  }

  /**
   * 数据处理时间
   * 1.日更新: 数据处理时间 = 数据生成时间
   * 2.周更新: 数据处理时间 = 数据生成时间
   * 3.月更新: 数据处理时间 = yyyyMM01(每个月一号)
   */
  def getProcessDate(day: String, period: String, periodDay: Int): String = {
    val date = LocalDate.parse(day, fmt)
    val value = (PeriodEnum.withName(period.toLowerCase()): @unchecked) match {
      case PeriodEnum.day => date
      case PeriodEnum.week => date.minusDays(date.getDayOfWeek.getValue - periodDay)
      case PeriodEnum.month => YearMonth.parse(day, fmt).atDay(periodDay)
      case PeriodEnum.year => Year.parse(day, fmt).atDay(periodDay)
    }
    fmt.format(value)
  }
}
