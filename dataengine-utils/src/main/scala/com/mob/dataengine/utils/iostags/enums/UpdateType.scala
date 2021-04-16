package com.mob.dataengine.utils.iostags.enums

/**
 * @author xlmeng
 */
object UpdateType extends Enumeration {

  type UpdateType = Value

  val incr: UpdateType = Value(0, "incr") // 增量
  val full: UpdateType = Value(1, "full") // 全量
  val zipper: UpdateType = Value(2, "zipper") // 拉链
  val snapshot: UpdateType = Value(3, "snapshot") // 快照

  def isFull(updateType: String): Boolean = UpdateType.withName(updateType).equals(full)

  def isIncr(updateType: String): Boolean = UpdateType.withName(updateType).equals(incr)
}
