package org.apache.spark.mob

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils
import org.apache.spark.{Partitioner, RangePartitioner, SparkEnv}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

/**
 * parallelism = pow(2,n) ; n in [0,16]
 *
 * @author juntao zhang
 */
class HfilePartitioner(parallelism: Int = 4) extends Partitioner {
  //  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = 256 * parallelism

//  def splitKeysNumber: Int = numPartitions - 1
//
//  lazy val keyLength: Int = {
//    var t = splitKeysNumber
//    var len = 0
//    while (t > 0) {
//      t >>>= 4
//      len += 1
//    }
//    len
//  }

  //  lazy val span: Int = (Math.pow(16, keyLength) / numPartitions).intValue()
  lazy val span: Int = 0xff / parallelism + 1

  //  lazy val md5SplitKeys: Array[Array[Byte]] = TableUtils.getMD5SplitKeys(numPartitions, "str")

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ =>
      val a = key.asInstanceOf[Array[Byte]]
      (a(0) & 0xff) * parallelism + (a(1) & 0xff) / span
  }

  //  def getPartition(key: Any): Int = key match {
  //    case null => 0
  //    case _ =>
  //      val a = key.asInstanceOf[Array[Byte]]
  //      val b = a(1) & 0xff
  //      val c = if (b < 0x80) {
  //        0
  //      } else {
  //        1
  //      }
  //      (a(0) & 0xff) * 2 + c
  //  }

  //  def getPartition(key: Any): Int = key match {
  //    case null => 0
  //    case _ =>
  //      val a = key.asInstanceOf[Array[Byte]]
  //      val b = a(1) & 0xff
  //      val c = if (b < 0x40) {
  //        0
  //      } else if (b < 0x80) {
  //        1
  //      } else if (b < 0xc0) {
  //        2
  //      } else {
  //        3
  //      }
  //      (a(0) & 0xff) * 4 + c
  //  }

  //  def getPartition(key: Any): Int = key match {
  //    case null => 0
  //    case _ =>
  //      val a = key.asInstanceOf[Array[Byte]]
  //      val b = a(1) & 0xff
  //      val c = if (b < 0x10) {
  //        0
  //      } else if (b < 0x20) {
  //        1
  //      } else if (b < 0x30) {
  //        2
  //      } else if (b < 0x40) {
  //        3
  //      } else if (b < 0x50) {
  //        4
  //      } else if (b < 0x60) {
  //        5
  //      } else if (b < 0x70) {
  //        6
  //      } else if (b < 0x80) {
  //        7
  //      } else if (b < 0x90) {
  //        8
  //      } else if (b < 0xa0) {
  //        9
  //      } else if (b < 0xb0) {
  //        10
  //      } else if (b < 0xc0) {
  //        11
  //      } else if (b < 0xd0) {
  //        12
  //      } else if (b < 0xe0) {
  //        13
  //      } else if (b < 0xf0) {
  //        14
  //      } else {
  //        15
  //      }
  //      (a(0) & 0xff) * 16 + c
  //  }

  //  def getPartition(key: Any): Int = key match {
  //    case null => 0
  //    case _ =>
  //      val a = key.asInstanceOf[Array[Byte]]
  //      val b = a(1) & 0xff
  //      val c = if (b < 0x20) {
  //        0
  //      } else if (b < 0x40) {
  //        1
  //      } else if (b < 0x60) {
  //        2
  //      } else if (b < 0x80) {
  //        3
  //      } else if (b < 0xa0) {
  //        4
  //      } else if (b < 0xc0) {
  //        5
  //      } else if (b < 0xe0) {
  //        6
  //      } else {
  //        7
  //      }
  //      (a(0) & 0xff) * 8 + c
  //  }


  //  def getPartition(key: Any): Int = key match {
  //    case null => 0
  //    case _ =>
  //      val a = key.asInstanceOf[Array[Byte]]
  //      var t = keyLength - 1
  //      var idx = 0
  //      while (t > 1) {
  //        idx += ((a(keyLength - t) & 0xff) / 16 * 4)
  //        t -= 1
  //      }
  //      //      (a(0) & 0xf) * 4 * 4 + (a(1) & 0xf) * 4 + a(2) & 0xf / 0x4
  //      idx + (a(keyLength - 1) & 0xff) / 16 / span
  //  }

  override def equals(other: Any): Boolean = other match {
    case h: HfilePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

  implicit var ordering = new math.Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = compareTo(a, b)
  }

  def compareTo(a: Array[Byte], b: Array[Byte]): Int = {
    val len1 = a.length
    val len2 = b.length
    val lim = Math.min(len1, len2)
    val v1 = a
    val v2 = b
    var k = 0
    while (k < lim) {
      val c1 = v1(k) & 0xff
      val c2 = v2(k) & 0xff
      if (c1 != c2) {
        return c1 - c2
      }
      k += 1
    }
    len1 - len2
  }
}

class CusRangePartitioner(
  hbasePartitionsPath: String,
  partitions: Int,
  rdd: RDD[String],
  private var ascending: Boolean = true)
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[String]]

  // An array of upper bounds for the first (partitions - 1) partitions
  var rangeBounds: Array[String] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd, sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(String, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd, imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, partitions)
      }
    }
  }

  val hbaseRegionIdx: immutable.IndexedSeq[String] = rangeBounds.indices.filter(i => (i + 1) % 10 == 0).map(
    i => rangeBounds(i)
  )

  cacheData(hbasePartitionsPath, hbaseRegionIdx.toArray)

  private def cacheData(path: String, data: Array[String]): Unit = {
    val hadoopConf = rdd.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    fs.delete(hdfsPath, true)
    val fin = fs.create(hdfsPath)
    fin.writeUTF(data.mkString("\u0001"))
    fs.close()
  }

  def numPartitions: Int = {
    rangeBounds.length + 1
  }

  //  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]
  private var binarySearch: (Array[String], String) => Int = binarySearch0

  // notice
  // 云龙对此作了hbase region适配 => [a,b) 原算法java.util.Arrays.binarySearch是(a,b]
  def binarySearch0(a: Array[String], key: String): Int = {
    var low = 0
    var high = a.length - 1
    while (low <= high) {
      val mid = (low + high) >>> 1
      val midVal = a(mid)
      val cmp = midVal.compareTo(key)
      if (cmp < 0) low = mid + 1
      else if (cmp > 0) high = mid - 1
      else return mid + 1 // key found
    }
    -(low + 1) // key not found.
  }

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition - 1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: CusRangePartitioner =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[String]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[String]]
        binarySearch = in.readObject().asInstanceOf[(Array[String], String) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag: ClassTag[Array[String]] = ds.readObject[ClassTag[Array[String]]]()
          rangeBounds = ds.readObject[Array[String]]()
        }
    }
  }
}
