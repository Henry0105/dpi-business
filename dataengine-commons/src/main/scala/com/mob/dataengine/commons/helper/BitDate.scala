package com.mob.dataengine.commons.helper

import java.time.YearMonth

import com.mob.dataengine.commons.enums.AppStatus
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
 * 长度为13的Int数组
 * 0:表示当前月份, 1-12:正常月份, 每个月首先将0写入真实月份,清除0,当前月值再写入0
 * e.g [9,0,0,5...,0](3月31日结果) => [0,0,0,9...,0](4月1日过程) => [1,0,0,9...,0](4月1日结果)
 * 0-31位表示每个月的日期是否存在状态
 * 用｜来加入状态,&来验证状态是否存在
 */
object BitDate {

  val BitDate_SIZE: Int = 13

  def main(args: Array[String]): Unit = {
    //    println(calDayInt(7))
    //    println(calMonthBit(2020, 2).toBinaryString)
    //    println(calMonthBit(2020, 3).toBinaryString)
    //    println(calSectionBit(1, 3).toBinaryString)
    //    val (hitArr, months) = genHitArrInfo("20191102", "20201107", "20201107")
    //    val f1 = hitBitDateArr(hitArr, months) _
    //    println(hitArr.mkString(","))
    //    println(f1(Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0), true))
    //    println(f1(Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7), false))

    val f2 = addToBitDateArr(0, 4) _
    println(f2(Seq(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 1))
  }

  /**
   * 算出指定天的bit表示对应的整型值
   */
  def calDayInt(day: Int): Int = {
    1 << (day - 1)
  }

  /**
   * 解析出月份 && 算出指定天的bit表示对应的整型值 当月的写入0索引
   */
  def calMonthAndDayInt(y: Int, m: Int)(day: String): (Int, Int) = {
    val date = DateUtils.getDate(day)
    val month = if (date.getMonthValue == m && date.getYear == y) {
      0
    } else {
      date.getMonthValue
    }
    (month, calDayInt(date.getDayOfMonth))
  }


  /**
   * 计算出指定年月的bit表示对应的整型值
   */
  def calMonthBit(year: Int, month: Int): Int = {
    calSectionBit(1, YearMonth.of(year, month).lengthOfMonth())
  }

  /**
   * 计算出时间区间(同一个月内)的bit表示对应的整型值
   */
  def calSectionBit(startDay: Int, endDay: Int): Int = {
    Range(startDay, endDay + 1).map(calDayInt).sum
  }

  /**
   * 生成用来对撞的校验数组 在[startTime, endTime]内所有位设为1
   */
  def genHitArrInfo(startTime: String, endTime: String, curDay: String): (Seq[Int], Seq[Int]) = {
    val hitArr = Array.fill(BitDate_SIZE)(0)
    val months = ArrayBuffer[Int]()
    val (y1, m1, d1) = DateUtils.getYearMonthDay(startTime)
    val (y2, m2, d2) = DateUtils.getYearMonthDay(endTime)
    val (y3, m3, _) = DateUtils.getYearMonthDay(curDay)

    val endM = if (y2 == y3 && m2 == m3) 0 else m2

    if (YearMonth.of(y1, m1) == YearMonth.of(y2, m2)) {
      // startTime和endTime在同一个月内
      hitArr(endM) = calSectionBit(d1, d2)
      months.append(endM)
    } else {
      // startTime和endTime在不同月份
      val startDate = DateUtils.getDate(startTime)
      hitArr(m1) = calSectionBit(d1, startDate.lengthOfMonth())
      months.append(m1)

      var curDate = startDate.plusMonths(1)
      while (curDate.getMonthValue != m2) {
        hitArr(curDate.getMonthValue) = calMonthBit(curDate.getYear, curDate.getMonthValue)
        months.append(curDate.getMonthValue)
        curDate = curDate.plusMonths(1L)
      }

      hitArr(endM) = calSectionBit(1, d2)
      months.append(endM)
    }
    (hitArr, months)
  }

  /* ---------------------------------  udf --------------------------------- */

  /**
   * 更新一个日期进入bitDateArr, 这里做成柯里化
   */
  def addToBitDateArr(month: Int, dayBit: Int)(bitDateArr: Seq[Int], isUpdate: Int): Seq[Int] = {
    if (isUpdate != 0) {
      if (bitDateArr == null) {
        val _bitDateArr = new Array[Int](13)
        _bitDateArr.updated(month, dayBit)
      } else {
        bitDateArr.updated(month, bitDateArr(month) + dayBit)
      }
    } else {
      bitDateArr
    }
  }

  /**
   * 补充bitDate的当月数据
   */
  def fillCurMonth(month: Int)(statusBitDates: Map[Int, Seq[Int]]): Map[Int, Seq[Int]] = {
    statusBitDates.mapValues(xs => xs.updated(0, xs(month)))
  }


  /**
   * 用hitArr对撞bitDateArr在,来查看bitDateArr在[,]时间区间是否存在
   * 1.include
   * 1000 & 1001 = 1000
   * 循环逻辑:之前月份不存在,检查当月是否存在,存在结束(true),不存在继续迭代(迭代最终为false)
   * 2.exclude
   * 1000 & 0001 = 0000
   * 循环逻辑:之前月份不存在,检查当月是否不存在,存在结束(false),不存在继续迭代(迭代最终为true)
   */
  def hitBitDateArr(hitArr: Seq[Int], months: Seq[Int], include: Boolean)(bitDateArr: Seq[Int]): Boolean = {
    if (bitDateArr == null) {
      return false
    }
    if (include) {
      var include = false
      for (month <- months if !include) include = (hitArr(month) & bitDateArr(month)) != 0
      include
    } else {
      var exclude = true
      for (month <- months if exclude) exclude = (hitArr(month) & bitDateArr(month)) == 0
      exclude
    }
  }

  def replaceAndRemoveOldMonth(oldMonth: Int)(bitDateArr: Seq[Int]): Seq[Int] = {
    if (bitDateArr == null) {
      null
    } else {
      bitDateArr.updated(oldMonth, bitDateArr.head).updated(0, 0)
    }
  }

  def updateUpperTime(bound: String)(upperTime: String): String = {
    if (bound < upperTime) upperTime else null
  }

  /* ---------------------------------  udaf --------------------------------- */
  class ConstructBitDates(y: Int, m: Int) extends UserDefinedAggregateFunction {
    def f: String => (Int, Int) = calMonthAndDayInt(y, m)

    override def inputSchema: StructType =
      StructType(StructField("status", IntegerType) :: StructField("day", StringType) :: Nil)

    override def bufferSchema: StructType = {
      StructType(StructField("status_bitDates", MapType(IntegerType, ArrayType(IntegerType))) :: Nil)
    }

    override def dataType: DataType = MapType(IntegerType, ArrayType(IntegerType))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Map.empty[Int, Seq[Int]]

    private def updateBitDate(map: Map[Int, Seq[Int]], key: Int, month: Int, dayInt: Int): Map[Int, Seq[Int]] = {
      val vOp = map.get(key)
      val newValue = if (vOp.isDefined) {
        val value = vOp.get
        value.updated(month, value(month) | dayInt)
      } else {
        val value = new Array[Int](13)
        value(month) = dayInt
        value.toSeq
      }
      map.updated(key, newValue)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val map = buffer.getAs[Map[Int, Seq[Int]]](0)
      val status = input.getAs[Int](0)
      val day = input.getAs[String](1)
      val (month, dayInt) = f(day)
      buffer(0) = status match {
        case -1 =>
          updateBitDate(map, AppStatus.uninstall.id, month, dayInt)
          updateBitDate(map, AppStatus.installed.id, month, dayInt)
        case 0 =>
          updateBitDate(map, AppStatus.installed.id, month, dayInt)
        case 1 =>
          updateBitDate(map, AppStatus.installed.id, month, dayInt)
          updateBitDate(map, AppStatus.newInstall.id, month, dayInt)
        case 2 =>
          updateBitDate(map, AppStatus.installed.id, month, dayInt)
        case 3 =>
          updateBitDate(map, AppStatus.active.id, month, dayInt)
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      if (!buffer2.isNullAt(0)) {
        val b1 = buffer1.getAs[Map[Int, Seq[Int]]](0)
        val b2 = buffer2.getAs[Map[Int, Seq[Int]]](0)
        b2.foreach { case (k, v) =>
          val op = b1.get(k)
          if (op.isDefined) {
            val arr = op.get
            for (i <- 0 until BitDate_SIZE) {
              v.updated(i, v(i) | arr(i))
            }
          }
        }
        buffer1.update(0, b1 ++ b2)
      }
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getAs[Map[Int, Seq[Int]]](0)
    }
  }

}
