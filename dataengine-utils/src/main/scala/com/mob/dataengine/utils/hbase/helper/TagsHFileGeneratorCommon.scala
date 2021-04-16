package com.mob.dataengine.utils.hbase.helper

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}

import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.utils.hbase.HFilePartitioner
import com.mob.dataengine.utils.tags.profile.TagsGeneratorHelper.{individualProfile, profileCategory, profileMetadata}
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HConstants, KeyValue}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * @author xlmeng
 */
abstract class TagsHFileGeneratorCommon(@transient val spark: SparkSession, day: String)
  extends TagsHFileGeneratorUDFCollections with Serializable {

  Seq(individualProfile, profileMetadata, profileCategory).foreach(tb =>
    spark.read.jdbc(url, tb, properties).createOrReplaceTempView(tb)
  )

  def transformData(full: Boolean): DataFrame


  /**
   * 目前只对这种格式的数据起作用
   * |-- device: string (nullable = true)
   * |-- features: map (nullable = true)
   * |    |-- key: string
   * |    |-- value: string (valueContainsNull = true)
   * `key` 作为hbase的列名,默认加前缀 [c_]
   * `value` 是一个string类型
   *
   * @param spark         : sparkSession
   * @param _df           : 源数据的DF
   * @param rowKey        : rowKey对应的df的列名,其中的值作为hbase的rowKey
   * @param zk            : 要导数据的hbase的zk地址,用来配置conf
   * @param numPartitions : hbase的分区数
   * @param hdfsPath      : 生成的hfile的地址
   * @param prefix        : 列名的前缀
   */
  def genHFile(spark: SparkSession, _df: DataFrame, rowKey: String, zk: String,
               numPartitions: Int, hdfsPath: String, prefix: String): Unit = {
    @transient lazy val hbaseConf: Configuration = {
      val conf = spark.sparkContext.hadoopConfiguration
      conf.set("hbase.zookeeper.quorum", zk)
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.mapreduce.hfileoutputformat.datablock.encoding", DataBlockEncoding.FAST_DIFF.name())
      conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName)
      conf.set(HConstants.HREGION_MAX_FILESIZE, (10 * HConstants.DEFAULT_MAX_FILE_SIZE).toString)
      conf.set("hbase.hfileoutputformat.families.bloomtype", s"c=${HColumnDescriptor.DEFAULT_BLOOMFILTER}")
      conf
    }

    val partitioner = HFilePartitioner(numPartitions)
    spark.udf.register("get_partition", (id: String) => {
      partitioner.getPartition(Hex.decodeHex(id.toCharArray.slice(0, 4)))
    })

    val df = _df.withColumn(
      "no_hash_par",
      callUDF("get_partition", col(rowKey))
    ).repartition(
      numPartitions, col("no_hash_par")
    ).sortWithinPartitions(col(rowKey))

    val pField = df.schema.fieldNames.filterNot(_.equals(rowKey)).head

    val cf: Array[Byte] = Bytes.toBytes("c")
    val timestamp: Long = convertDayToLong(day)
    val totalAccumulator = spark.sparkContext.longAccumulator("total")

    df.rdd.flatMap {
      row =>
        val arr = ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()
        val rk = row.getAs[String](rowKey)
        val profileInfo = row.getAs[Map[String, String]](pField)
        val rkb = if (true) {
          Hex.decodeHex(rk.toCharArray)
        } else {
          rk.getBytes()
        }
        val rkw = new ImmutableBytesWritable(rkb)

        profileInfo.toSeq.sortBy(_._1).foreach { case (cate2Id, profileId) =>
          arr += ((rkw, new KeyValue(
            rkb,
            cf,
            Bytes.toBytes(cate2Id),
            timestamp,
            Bytes.toBytes(profileId)
          )))
        }

        totalAccumulator.add(1)
        arr
    }.saveAsNewAPIHadoopFile(
      hdfsPath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    println(s"hfile $rowKey num: ${totalAccumulator.value}")
  }

  def convertDayToLong(day: String): Long = {
    val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
    val date = LocalDate.parse(day, fmt)
    date.atStartOfDay(ZoneOffset.ofHours(8)).toInstant.toEpochMilli
  }

  /**
   * 得到 map(profile_id -> 2级分类id)
   */
  val profileIdToLevel2CategoryId: Map[String, String] = {
    @tailrec
    def findLevel2ParentId(categoryId: Int, cateId2LevelParentId: Map[Int, (Int, Int)]): Int = {
      val Level2: Int = 2
      cateId2LevelParentId.get(categoryId) match {
        case Some(x) =>
          if (x._1 <= Level2) categoryId else findLevel2ParentId(x._2, cateId2LevelParentId)
        case None => 0
      }
    }

    Seq(individualProfile, profileMetadata, profileCategory).foreach(tb =>
      spark.read.jdbc(url, tb, properties).createOrReplaceTempView(tb)
    )

    import spark.implicits._
    val profileId2CategoryId = sql(
      s"""
         |SELECT concat(a.profile_id, '_', a.profile_version_id) as full_id
         |     , b.profile_category_id
         |FROM   $individualProfile as a
         |INNER JOIN $profileMetadata as b
         |   ON b.is_avalable = 1 AND b.is_visible = 1 AND a.profile_id = b.profile_id
         |WHERE a.profile_datatype IN ('int', 'string', 'boolean', 'double', 'bigint')
       """.stripMargin)
      .map(r => {
        val fullId = r.getAs[String]("full_id")
        val profileCategoryId = r.getAs[Int]("profile_category_id")
        fullId -> profileCategoryId
      }).collect().toMap

    val cateId2LevelParentId = MetadataUtils.findProfileCategory().map(pc => (pc.id, (pc.level, pc.parentId))).toMap

    profileId2CategoryId.map { case (id, categoryId) =>
      id -> s"cateid_${findLevel2ParentId(categoryId, cateId2LevelParentId)}"
    }
  }

  def taglistLikeCategoryIdBroadcast(ids: Array[String], profileIdToLevel2CategoryId: Map[String, String]):
  Broadcast[Map[String, Map[String, Seq[String]]]] = {
    val bcValue = ids.map(id => {
      profileIdToLevel2CategoryId.get(id)
    }).filter(_.isDefined).distinct.map(op => (op.get, null)).toMap[String, Map[String, Seq[String]]]
    spark.sparkContext.broadcast(bcValue)
  }

}