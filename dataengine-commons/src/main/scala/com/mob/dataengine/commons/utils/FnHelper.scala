package com.mob.dataengine.commons.utils

object FnHelper {
  // 写hive表的时候计算一下coalesce的分区数
  def getCoalesceNum(total: Long, threshold: Int = 3000000): Int = {
    val t = (total / threshold).toInt
    if (t > 0) t else 1
  }
}
