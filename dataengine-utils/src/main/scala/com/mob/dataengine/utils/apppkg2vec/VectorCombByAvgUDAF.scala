package com.mob.dataengine.utils.apppkg2vec

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

// 向量取均值合并
object VectorCombByAvgUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("feature", ArrayType(DoubleType)) :: Nil)
  override def bufferSchema: StructType = StructType(StructField("feature", ArrayType(DoubleType)) :: Nil)
  override def dataType: DataType = ArrayType(DoubleType)
  override def deterministic: Boolean = true
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, Seq.empty[Double])
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
    buffer.getAs[Seq[Double]](0).length match {
      case 0 => buffer.update(0, input.getAs[Seq[Double]](0))
      case _ => buffer.update(0, input.getAs[Seq[Double]](0).zip(buffer.getAs[Seq[Double]](0)
                     ).map { case (d1, d2) => (d1 + d2) / 2.0 })
    }
  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit =
    buffer.getAs[Seq[Double]](0).length match {
      case 0 => buffer.update(0, input.getAs[Seq[Double]](0))
      case _ => buffer.update(0, input.getAs[Seq[Double]](0).zip(buffer.getAs[Seq[Double]](0)
                     ).map { case (d1, d2) => (d1 + d2) / 2.0 })
  }
  override def evaluate(buffer: Row): Any = buffer.getAs[Seq[Double]](0)
}
