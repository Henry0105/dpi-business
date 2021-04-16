package com.mob.dataengine.commons.exceptions

/**
 * @author juntao zhang
 */
case class DataengineException(msg: String, code: Int) extends Exception(msg) {
  def this(msg: String) = {
    this(msg, 1)
  }
}
