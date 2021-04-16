package com.mob.dataengine.utils.apppkg2vec

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}

import scala.collection.JavaConverters._
import scala.collection.generic.Growable
import scala.collection.immutable

/**
 * From org.apache.spark.util.BoundedPriorityQueue
 * Bounded priority queue. This class wraps the original PriorityQueue
 * class and modifies it such that only the top K elements are retained.
 * The top K elements are defined by an implicit Ordering[A].
 * 由于原类的使用范围限制, 所以提取出来使用(完全拷贝)
 * 该类是在PriorityQueue基础上的包装类, 可自动排序并维持队列中的元素数量, 排序器由初始化时传入
 */
class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  override def size: Int = underlying.size

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem: A): this.type = {
    if (size < maxSize) {
      underlying.offer(elem)
    } else {
      maybeReplaceLowest(elem)
    }
    this
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear() { underlying.clear() }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else {
      false
    }
  }
}

object BoundedPriorityQueue {

  /**
   * 测试效率: 最好情况 ≈ 3倍差距
   * sort + take [37ms]
   * queue [13ms]
   */
  def main(args: Array[String]): Unit = {

    val array: immutable.Seq[(String, Double)] =
      (1 to 100000).reverse.map(_.toString).toSeq.zip((1 to 100000).reverse.map(_.toDouble))
    val start = System.currentTimeMillis()

    //val res: immutable.Seq[(String, Double)] = array.sortBy(_._2)(Ordering.Double.reverse).take(30)
    implicit object TupleOrd extends math.Ordering[(String, Double)] {
      override def compare(x: (String, Double), y: (String, Double)) =
        if (x._2 > y._2) 1 else if (x._2 < y._2) -1 else 0
    }
    val res = new BoundedPriorityQueue[(String, Double)](30)
    array.foreach(res += _)
    val end = System.currentTimeMillis()
    println(s"costs[${end - start}]\n${res.mkString("|")}")
  }
}
