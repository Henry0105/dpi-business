package org.apache.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.{PartitionCoalescer, PartitionGroup, PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

/**
 * @author juntao zhang
 */
class MobRangeCoalescer(parallelism: Int) extends PartitionCoalescer with Serializable {

  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {

    println(s"parallelism=>$parallelism")
    println(s"maxPartitions=>$maxPartitions")
    println(s"parent.partitions.length=>${parent.partitions.length}")

    val partitions = parent.partitions
    val groups = new Array[PartitionGroup](maxPartitions)
    (0 until maxPartitions).foreach(i => {
      groups(i) = new PartitionGroup()
    })

    partitions.indices.map(i => {
      groups(i / parallelism).partitions += partitions(i)
    })

    groups.indices.foreach(i => {
      val g = groups(i)
    })
    groups
  }
}

class MobRangePartitioner(
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
    case r: MobRangePartitioner =>
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
