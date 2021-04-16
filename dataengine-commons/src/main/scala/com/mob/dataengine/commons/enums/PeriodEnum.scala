package com.mob.dataengine.commons.enums

/**
 * @author xlmeng
 */
object PeriodEnum extends Enumeration {

  type PeriodEnum = PeriodEnum.Value

  val day: PeriodEnum = Value(0, "day")
  val week: PeriodEnum = Value(1, "week")
  val month: PeriodEnum = Value(2, "month")
  val year: PeriodEnum = Value(3, "year")
}
