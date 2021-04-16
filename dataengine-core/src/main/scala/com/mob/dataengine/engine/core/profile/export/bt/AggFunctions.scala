package com.mob.dataengine.engine.core.profile.export.bt

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * @author juntao zhang
 */
class AggFunction() extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    StructType(Array(
      StructField("type", StringType),
      StructField("day", StringType),
      StructField("value", StringType),
      StructField("needed", ArrayType(StringType))
    ))
  }

  override def bufferSchema: StructType = new StructType().add(
    StructField("r1", MapType(StringType, ArrayType(StringType)))
  ).add(
    StructField("r2", ArrayType(StringType))
  )


  override def dataType: StructType = new StructType().add(
    StructField("features", MapType(StringType, ArrayType(StringType)))
  ).add(
    StructField("features_needed", ArrayType(StringType))
  )

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, mutable.HashMap[String, Seq[String]]())
    buffer.update(1, Seq[String]())
  }

  def merge(
    m1: collection.Map[String, Seq[String]],
    m2: collection.Map[String, Seq[String]]
  ): mutable.HashMap[String, Seq[String]] = {
    val m = new mutable.HashMap[String, Seq[String]]()
    m1.foreach { case (k, v) => m.put(k, v) }
    m2.foreach { case (k, v) =>
      if (!m.isDefinedAt(k) || m(k).head < v.head) {
        m.put(k, v)
      }
    }
    m
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val m = buffer.getMap[String, Seq[String]](0)
    val k = input.getString(0)
    val day = input.getString(1)
    val v = input.getString(2)
    val s = input.getSeq[String](3)
    if (!m.isDefinedAt(k) || m(k).head < day) {
      buffer.update(0, m.updated(k, Seq(day, v)))
      buffer.update(1, (s ++ buffer.getSeq[String](1)).distinct)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val m1 = buffer1.getMap[String, Seq[String]](0)
    val m2 = buffer2.getMap[String, Seq[String]](0)
    buffer1.update(0, merge(m1, m2))
    buffer1.update(1, (buffer1.getSeq[String](1) ++ buffer2.getSeq[String](1)).distinct)
  }

  override def evaluate(buffer: Row): Any = {
    val result = buffer.getMap[String, Seq[String]](0)
    val tags = buffer.getSeq[String](1)
    Row(result.toMap, (tags.toSet -- result.keys).toSeq)
  }
}

class MapsAggFunction() extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    StructType(Array(
      StructField("map", MapType(StringType, ArrayType(StringType)))
    ))
  }

  override def bufferSchema: StructType = new StructType().add(
    StructField("result", MapType(StringType, ArrayType(StringType)))
  )

  override def dataType: DataType = MapType(StringType, ArrayType(StringType))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, mutable.HashMap[String, Seq[String]]())
  }

  def merge(
    m1: collection.Map[String, Seq[String]],
    m2: collection.Map[String, Seq[String]]
  ): mutable.HashMap[String, Seq[String]] = {
    val m = new mutable.HashMap[String, Seq[String]]()
    m1.foreach { case (k, v) => m.put(k, v) }
    m2.foreach { case (k, v) =>
      if (!m.isDefinedAt(k) || m(k).head < v.head) {
        m.put(k, v)
      }
    }
    m
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.isNullAt(0)) {
      return
    }
    val m1 = buffer.getMap[String, Seq[String]](0)
    val m2 = input.getMap[String, Seq[String]](0)
    buffer.update(0, merge(m1, m2))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val m1 = buffer1.getMap[String, Seq[String]](0)
    val m2 = buffer2.getMap[String, Seq[String]](0)
    buffer1.update(0, merge(m1, m2))
  }

  override def evaluate(buffer: Row): Any = {
    val result = buffer.getMap[String, Seq[String]](0)
    result.toMap
  }
}

