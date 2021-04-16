package com.mob.dataengine.utils.tags.profile

import com.mob.dataengine.utils.tags.MaxAccumulator
import org.apache.spark.sql.SparkSession

case class HiveTable(
  database: String,
  table: String,
  key: String,
  isPartitionTable: Boolean,
  whereClause: String,
  partitionClause: String
) {
  val acc: MaxAccumulator = if (isPartitionTable) null else new MaxAccumulator

  def fullTableName: String = s"$database.$table"

  def isTimewindowTable: Boolean = {
    table.contains("timewindow")
  }

  def updateTimeField(): String = {
    if (isPartitionTable) {
      """(\w+)=(\d+)""".r.findFirstMatchIn(partitionClause).get.subgroups.head
    } else {
      table match {
        case "rp_device_profile_full_view" => "processtime_all"
        case _ => "processtime"
      }
    }
  }

  def updateTime(): String = {
    if (isPartitionTable) {
      """(\w+)=(\d+)""".r.findFirstMatchIn(partitionClause).get.subgroups(1)
    } else {
      acc.value.toString
    }
  }

  def updateTimeClause(lastTime: String, updateTime: String, fn: String = ""): String = {
    val field = updateTimeField()
    if (isPartitionTable) {
      s"""
         |if($field > '$lastTime', '$updateTime', $field)
       """.stripMargin
    } else {  // 无时间分区的字段需要使用udf来记录最后更新时间
      s"""
         |if($fn($field) > '$lastTime', '$updateTime', $field)
       """.stripMargin
    }
  }

  def registerUpdateTimeUDF(spark: SparkSession, fn: String): Unit = {
    spark.sparkContext.register(acc)
    spark.udf.register(fn, (updateTime: String) => {
      acc.add(updateTime.toLong)
      updateTime
    })
  }
}
