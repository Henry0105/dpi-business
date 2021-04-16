package com.mob.dataengine.core.utils

import java.io.PrintWriter
import com.mob.dataengine.core.constants.DataengineExceptionType.DataengineExceptionType


case class DataengineException(msg: String, code: Int) extends Exception(msg) {
  def this(msg: String) = {
    this(msg, 1)
  }
}
object DataengineExceptionUtils {
  def apply(msg: String, code: DataengineExceptionType): DataengineException = {
    new DataengineException(msg, code.id)
  }

  def stackTraceToString(e: Exception): String = {
    import java.io.CharArrayWriter
    val caw = new CharArrayWriter
    val pw = new PrintWriter(caw)
    e.printStackTrace(pw)
    val result = caw.toString
    caw.close()
    pw.close()
    result
  }
}
