package com.mob.dataengine.utils.idmapping.hive

import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * @author juntao zhang
 */
class AggregateIdTypeV2(limit: Int) extends UserDefinedAggregateFunction {

  //   指定输入的数据类型
  override def inputSchema: StructType = {
    StructType(Array(
      StructField("device", StringType),
      StructField("device_tm", StringType),
      StructField("device_ltm", StringType)
    ))
  }

  //   聚合结果类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("device_tm", MapType(StringType, types.ArrayType(StringType)))))
  }

  //   返回的数据类型
  override def dataType: DataType = {
    StructType(Array(StructField("device", StringType), StructField("device_tm", StringType),
      StructField("device_ltm", StringType), StructField("update_time", StringType)))
  }

  override def deterministic: Boolean = true

  //   在聚合之前初始化结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new mutable.HashMap[String, Seq[String]]() {
      override def initialSize (): Int = 2
    }
  }

  //   map侧合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var tmMap = buffer.getMap[String, Seq[String]](0)
    // 取最新的200个
    if (input.getString(0) != null && input.getString(0).nonEmpty) {

      val maxByLtm = input.getString(0).split(",").zip(input.getString(1).split(","))
        .zip(input.getString(2).split(",")).map(x => (x._1._1, x._1._2, x._2))

      maxByLtm.foreach{ case (k, tm, ltm) =>
        tmMap = updateTmLtm(tmMap, k, tm, ltm)
      }
      val newM = tmMap.toSeq.sortWith((a, b) => a._2(1) > b._2(1)).slice(0, limit).toMap

      buffer.update(0, newM)
    }
  }

  //   reduce测合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var m1 = buffer1.getMap[String, Seq[String]](0)
    val m2 = buffer2.getMap[String, Seq[String]](0)

    m2.foreach { case (k, Seq(tm, ltm)) =>
      m1 = updateTmLtm(m1, k, tm, ltm)
    }

    val result = m1.toSeq.sortWith((a, b) => a._2(1) > b._2(1)).slice(0, limit).toMap // 取最新的200个

    buffer1.update(0, result)
  }

  def updateTmLtm(m: scala.collection.Map[String, Seq[String]], k: String, tm: String,
    ltm: String): scala.collection.Map[String, Seq[String]] = {
    if (!m.contains(k)) {
      m.updated(k, Seq(tm, ltm))
    } else if (m(k)(1) < ltm) {
      val newTM = if (m(k).head > tm) tm else m(k).head
      m.updated(k, Seq(newTM, ltm))
    } else if (m(k).head > tm) {
      val newLtm = if (m(k)(1) < ltm) ltm else m(k)(1)
      m.updated(k, Seq(tm, newLtm))
    } else {
      m
    }
  }

  //   返回udaf最后的结果
  override def evaluate(buffer: Row): Any = {
    val (deviceIt, tmIt) = buffer.getMap[String, Seq[String]](0).unzip
    val devices = deviceIt.mkString(",")
    val times = tmIt.map(_.head).mkString(",")
    val ltimes = tmIt.map(_(1)).mkString(",")

    if (tmIt.nonEmpty) {
      Row(devices, times, ltimes, tmIt.map(_(1)).max)
    } else {
      if (deviceIt.isEmpty) {
        Row(null, null, null, null)
      } else {
        Row(devices, null, null, null)
      }
    }
  }
}
