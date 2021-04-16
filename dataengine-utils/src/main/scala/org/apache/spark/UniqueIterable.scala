package org.apache.spark

import org.apache.spark.sql.Row

import scala.annotation.tailrec

/**
 * @author juntao zhang
 */
class UniqueIterable(val it: Iterator[Row]) extends Iterator[Row] with Serializable {
  var nextE: Option[Row] = None
  var last: String = ""

  def hasNext: Boolean = {
    popNext()
    nextE.isDefined
  }

  def next: Row = {
    popNext()
    val res = nextE.get
    nextE = None
    res
  }

  @tailrec
  private def popNext() {
    if (nextE.isEmpty && it.hasNext) {
      val r = it.next
      if (last == r.getString(0)) popNext()
      else {
        last = r.getString(0)
        nextE = Some(r)
      }
    }
  }
}
