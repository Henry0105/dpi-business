package com.mob.dataengine.commons.utils

import java.util.{Calendar, Locale}

import org.apache.commons.lang3._
import org.apache.commons.lang3.time.DateFormatUtils

import scala.collection.mutable.ArrayBuffer

/**
 * @author juntao zhang
 */
object DateUtils {
  def format(time: Long): String = {
    DateFormatUtils.format(time, "yyyyMMdd")
  }

  def currentTime(): String = {
    DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")
  }

  def currentTime2(): String = {
    DateFormatUtils.format(System.currentTimeMillis(), "yyyyMMddHHmmss")
  }

  def currentDay(): String = {
    format(System.currentTimeMillis())
  }

  def ranges(start: String, end: String, daysNum: Int): Seq[(String, String)] = {
    if (start > end) {
      return Seq()
    }
    var t = end
    val c = Calendar.getInstance(Locale.CHINA)
    val r = ArrayBuffer[(String, String)]()
    do {
      c.clear()
      c.setTimeInMillis(time.DateUtils.parseDate(t, "yyyyMMdd").getTime)
      c.add(Calendar.DATE, -daysNum)
      val b = format(c.getTime.getTime)
      r += ((if (b > start) b else start, t))
      c.add(Calendar.DATE, -1)
      t = format(c.getTime.getTime)
    } while (t > start)
    r
  }

  /**
   * [20190307,20190203],2 => [20190306,20190305,20190202,20190201]
   * [20190307,20190306],2,false => [20190305,20190304]
   * [20190307,20190306],2,true => [20190307,20190306,20190305,20190304]
   */
  def getLastNDays(days: Seq[String], n: Int, containsCurr: Boolean = false): Seq[String] = {
    if (n == 0) {
      return days.sorted
    }
    val c = Calendar.getInstance(Locale.CHINA)
    val tmp = days.flatMap { d =>
      c.setTimeInMillis(time.DateUtils.parseDate(d, "yyyyMMdd").getTime)
      (0 until n).map(_ => {
        c.add(Calendar.DATE, -1)
        format(c.getTime.getTime)
      })
    }.toSet
    if (containsCurr) {
      (days ++ tmp).distinct.sorted
    } else {
      tmp.filterNot(days.contains).toList.sorted
    }
  }
}
