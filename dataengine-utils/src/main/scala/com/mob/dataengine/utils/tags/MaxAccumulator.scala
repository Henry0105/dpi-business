package com.mob.dataengine.utils.tags

import org.apache.spark.util.AccumulatorV2

class MaxAccumulator extends AccumulatorV2[Long, Long] with Serializable {
  private var _val = 0L

  override def isZero: Boolean = _val == 0L

  override def copy(): MaxAccumulator = {
    val newAcc = new MaxAccumulator
    newAcc._val = this._val
    newAcc
  }

  override def reset(): Unit = {
    _val = 0L
  }

  override def add(v: Long): Unit = {
    if (v > _val) {
      _val = v
    }
  }

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
    case o: MaxAccumulator =>
      if (o._val > _val) _val = o._val
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Long = _val
}
