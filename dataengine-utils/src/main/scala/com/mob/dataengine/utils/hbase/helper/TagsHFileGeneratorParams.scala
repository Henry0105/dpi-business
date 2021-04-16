package com.mob.dataengine.utils.hbase.helper

case class TagsHFileGeneratorParams(
                                     os: String = "",
                                     zk: String = "",
                                     numPartitions: Int = 1024,
                                     hdfsPath: String = "",
                                     rowKey: String = "",
                                     prefix: String = "",
                                     day: String = "",
                                     full: Boolean = false
                                   ) {
  override def toString: String =
    s"""
       |os=$os, zk=$zk, numPartitions=$numPartitions, hdfsPath=$hdfsPath
       |rowKey=$rowKey, prefix=$prefix, day=$day, full=$full
       |""".stripMargin
}