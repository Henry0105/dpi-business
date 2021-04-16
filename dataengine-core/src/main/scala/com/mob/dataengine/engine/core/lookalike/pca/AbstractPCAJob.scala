package com.mob.dataengine.engine.core.lookalike.pca

import java.util.Date

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

abstract class AbstractPCAJob {

  /**
   * 将输入日期格式化为"yyyyMMdd"字串
   *
   * @param date 输入日期
   * @return 格式化后的日期字符串
   */
  protected def format(date: Date): String = DateFormatUtils.format(date, "yyyyMMdd")

  /**
   * 指定天数差产生另一日期
   */
  protected def genAnotherDay(rankDate: String, dayStep: Int, yearStep: Int = 0): String = {
    val parsedDate = DateUtils.parseDate(rankDate,
      "yyyy年MM月dd日", "yyyy年MM月", "yyyy年", "yyyyMMdd", "yyyyMM", "yyyy", "yyyy-MM-dd")
    if (yearStep > 0) {
      format(DateUtils.addDays(DateUtils.addYears(parsedDate, -1), -23))
    } else format(DateUtils.addDays(parsedDate, dayStep))
  }

  /**
   * 获取当前时间, 格式为yyyy-MM-dd HH:mm:ss
   */
  protected def currTime: String = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")
}
