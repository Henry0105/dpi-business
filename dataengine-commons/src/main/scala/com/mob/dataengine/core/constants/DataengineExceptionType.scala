package com.mob.dataengine.core.constants

object DataengineExceptionType extends Enumeration with Serializable  {
  type DataengineExceptionType = Value
  val EXPORT_OUTPUT_PATH_IS_NULL: DataengineExceptionType.Value = Value(1)
  val API_HTTP_ERROR: DataengineExceptionType.Value = Value(2)
  val PARAMETER_ERROR: DataengineExceptionType.Value = Value(3)
  val READ_PATH_IS_NULL: DataengineExceptionType.Value = Value(4)
}
