package com.mob.dataengine.commons.traits

import com.mob.dataengine.commons.annotation.code.{explanation, sourceTable, targetTable, author, createTime}

@author("jiandd")
@createTime("2020-01-06")
@explanation("控制 partition 数量")
trait PartitionTrait extends Serializable{
  def getCoalesceNum(total: Long, threshold: Int = 3000000): Int = {
    val t = (total / threshold).toInt
    if (t > 0) t else 1
  }
}
