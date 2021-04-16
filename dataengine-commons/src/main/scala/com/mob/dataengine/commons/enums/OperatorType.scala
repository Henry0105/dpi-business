package com.mob.dataengine.commons.enums

object OperatorType extends Enumeration {
  type OperatorType = Value

  val GREATER_OR_EQUAL: OperatorType = Value(0, ">=")
  val GREATER: OperatorType = Value(1, ">")
  val LESS_OR_EQUAL: OperatorType = Value(2, "<=")
  val LESS: OperatorType = Value(3, "<")
  val EQUAL: OperatorType = Value(4, "=")
  val NOT_EQUAL: OperatorType = Value(5, "!=")

  val OR: OperatorType = Value(6, "|")
  val AND: OperatorType = Value(7, "&")

}
