package com.mob.dataengine.utils.hbase

import org.apache.spark.Partitioner

case class HFilePartitioner(numPartitions: Int) extends Partitioner {
  val multiple: Int = numPartitions / 256
  val span: Int = 256 / (if (multiple > 1) multiple else numPartitions)

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ =>
      val a = key.asInstanceOf[Array[Byte]]
      if (numPartitions > 256) {
        (a(0) & 0xff) * multiple + (a(1) & 0xff) / span
      } else {
        (a(0) & 0xff) / span
      }
  }
}
