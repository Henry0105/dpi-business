package com.mob.dataengine.utils.hbase

import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.traits.Logging
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.DateUtils
import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.{HColumnDescriptor, HConstants, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import scopt.OptionParser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TagsHFileGenerator {

  case class Params(
    zk: String = "",
    numPartitions: Int = 1024,
    hdfsPath: String = "",
    rowKey: String = "",
    prefix: String = "",
    endDay: String = "",
    span: Int = 0
  )

  def main(args: Array[String]): Unit = {
    /*
    val zk = "bd15-161-218,bd15-161-220,bd15-161-219"
    val numPartitions = 1024
    val hdfsPath = "/user/app360_test/tmp_menff/hfile_cols3"
    */

    val defaultParams: Params = Params()
    val projectName = s"TagsHFileGenerator[${DateUtils.currentDay()}]"

    val parser = new OptionParser[Params](projectName) {
      head(s"$projectName")
      opt[String]('z', "zk")
        .text("zk地址")
        .required()
        .action((x, c) => c.copy(zk = x))
      opt[Int]('n', "partitions")
        .text("hfile的个数")
        .required()
        .action((x, c) => c.copy(numPartitions = x))
      opt[String]('h', "hdfsPath")
        .text("hdfs的路径")
        .action((x, c) => c.copy(hdfsPath = x))
      opt[String]('p', "prefix")
        .text(s"hbase列名的前缀")
        .action((x, c) => c.copy(prefix = x))
      opt[String]('r', "rowKey")
        .text(s"rowKey的字段")
        .action((x, c) => c.copy(rowKey = x))
      opt[String]('d', "endDay")
        .text("要导入的数据")
        .action((x, c) => c.copy(endDay = x))
      opt[Int]('s', "span")
        .text("往后推几天")
        .action((x, c) => c.copy(span = x))
    }

    // 数据源目前只是回溯的数据源

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (fs.exists(new Path(params.hdfsPath))) {
          println("delete path: " + params.hdfsPath)
          fs.delete(new Path(params.hdfsPath), true)
        }

        val t = TagsHFileGenerator(spark)
        val startDay = DateUtils.getDayBefore(params.endDay, -1 * params.span)
        val df = t.expandTaglist(startDay, params.endDay)
        t.genHFile(spark, df, params.rowKey, params.zk, params.numPartitions,
          params.hdfsPath, params.prefix)
      case _ => sys.exit(1)
    }
  }
}

case class TagsHFileGenerator(spark: SparkSession) extends Logging {
  val taglistTagId2ProfileIdMap: Map[String, String] = MetadataUtils.findTaglistLikeProfiles("tag_list;")
  val catelistTagId2ProfileIdMap: Map[String, String] = MetadataUtils.findTaglistLikeProfiles("catelist;")

  val taglistTagId2ProfileIdMapBC: Broadcast[Map[String, String]] =
    spark.sparkContext.broadcast(taglistTagId2ProfileIdMap)
  val catelistTagId2ProfileIdMapBC: Broadcast[Map[String, String]] =
    spark.sparkContext.broadcast(catelistTagId2ProfileIdMap)

  // 对taglist进行展开
  def explodeTaglist(m: Map[String, Seq[String]]): Map[String, Seq[String]] = {
    m.flatMap{ case (profileId, value) =>
      profileId match {
        case "5919_1000" => mapTagId2ProfileId(value, taglistTagId2ProfileIdMapBC.value)  // taglist
        case "5920_1000" => mapTagId2ProfileId(value, catelistTagId2ProfileIdMapBC.value) // catelist
        case _ => Seq((profileId, value))
      }
    }
  }


  def expandTaglist(startDay: String, endDay: String): DataFrame = {
    spark.udf.register("explode_taglist", explodeTaglist _)
    spark.udf.register("agg_by_day", new AggTagsByDay)

    sql(
      s"""
         |select device, explode_taglist(agg_by_day(profile, day)) profile
         |from (
         |  select device, profile, day
         |  from ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V6}
         |  where day >= '$startDay' and day <= '$endDay'
         |) as a
         |where device rlike '[0-9a-f]{40,40}' and length(device) = 40 and length(day)=8
         |group by device
        """.stripMargin)

  }

  def mapTagId2ProfileId(arr: Seq[String], mapping: Map[String, String]): Seq[(String, Seq[String])] = {
    val day = arr.head
    val value = arr(1)

    val kv = value.split("=", 2)
    kv(0).split(",").zip(kv(1).split(",")).map { case (tagId, tagValue) =>
      (mapping(tagId), Seq(day, tagValue))
    }
  }

  /**
   * 目前只对这种格式的数据起作用
   * |-- device: string (nullable = true)
   * |-- features: map (nullable = true)
   * |    |-- key: string
   * |    |-- value: string (valueContainsNull = true)
   * `key` 作为hbase的列名,默认加前缀 [c_]
   * `value` 是一个string类型
   * @param spark: sparkSession
   * @param _df: 源数据的DF
   * @param rowKey: rowKey对应的df的列名,其中的值作为hbase的rowKey
   * @param zk: 要导数据的hbase的zk地址,用来配置conf
   * @param numPartitions: hbase的分区数
   * @param hdfsPath: 生成的hfile的地址
   * @param prefix: 列名的前缀
   */

  def genHFile(spark: SparkSession, _df: DataFrame, rowKey: String, zk: String,
    numPartitions: Int, hdfsPath: String, prefix: String): Unit = {
    @transient lazy val hbaseConf: Configuration = {
      val conf = spark.sparkContext.hadoopConfiguration
      conf.set("hbase.zookeeper.quorum", zk)
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.mapreduce.hfileoutputformat.datablock.encoding", DataBlockEncoding.FAST_DIFF.name())
      conf.set("hbase.hfileoutputformat.families.bloomtype", s"c=${HColumnDescriptor.DEFAULT_BLOOMFILTER}")
      conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName)
      conf.set(HConstants.HREGION_MAX_FILESIZE, (10 * HConstants.DEFAULT_MAX_FILE_SIZE).toString)
      conf
    }

    val partitioner = HFilePartitioner(numPartitions)
    spark.udf.register("get_partition", (id: String) => {
      partitioner.getPartition(Hex.decodeHex(id.toCharArray.slice(0, 4)))
    })

    val df = _df.withColumn(
      "no_hash_par",
      callUDF("get_partition", col("device"))
    ).repartition(
      numPartitions, col("no_hash_par")
    ).sortWithinPartitions(col("device"))

    val pField = df.schema.fieldNames.filterNot(_.equals(rowKey)).head

    val cf: Array[Byte] = Bytes.toBytes("c")
    val totalAccumulator = spark.sparkContext.longAccumulator("total")

    // 这里需要拿到所有的taglist和catelist对应的profileId
    val taglistProfileIdsBC = spark.sparkContext.broadcast(taglistTagId2ProfileIdMap.values.toSeq)
    val catelistProfileIdsBC = spark.sparkContext.broadcast(catelistTagId2ProfileIdMap.values.toSeq)

    df.rdd.flatMap {
      row =>
        val arr = ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()
        val rk = row.getAs[String](rowKey)
        val profileInfo = row.getAs[Map[String, Seq[String]]](pField)
        val rkb = if (true) {
          Hex.decodeHex(rk.toCharArray)
        } else {
          rk.getBytes()
        }
        val rkw = new ImmutableBytesWritable(rkb)
        // deal profile

        // 包括原来要插入的profileId和要删除的profileId
        val toInsertProfileIds = profileInfo.keys.toSeq
        var allProfileIds = ArrayBuffer[String](toInsertProfileIds: _*)

        if (toInsertProfileIds.intersect(taglistProfileIdsBC.value).nonEmpty) {
          allProfileIds = allProfileIds.union(taglistProfileIdsBC.value)
        }
        if (toInsertProfileIds.intersect(catelistProfileIdsBC.value).nonEmpty) {
          allProfileIds = allProfileIds.union(catelistProfileIdsBC.value)
        }

        allProfileIds.sorted.foreach{ profileId =>
          if (profileInfo.contains(profileId)) {
            val tmp = profileInfo(profileId)
            val day = tmp.head
            val value = tmp(1)
            if (StringUtils.isNotBlank(value)) {
              arr += ((rkw, new KeyValue(
                rkb,
                cf,
                Bytes.toBytes(s"$prefix$profileId"),
                Bytes.toBytes(s"$day" + s"\u0001$value")
              )))
            }
          } else {  // 这里删除的是老的taglist和catelist的值, 因为这次的数据可能没有上次的多, 需要将上次多的删除掉
            arr += ((rkw, new KeyValue(
              rkb,
              cf,
              Bytes.toBytes(s"$prefix$profileId"),
              HConstants.LATEST_TIMESTAMP,
              Type.DeleteColumn
            )))
          }
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
  }
}

class AggTagsByDay extends UserDefinedAggregateFunction {

  //   指定输入的数据类型
  override def inputSchema: StructType = {
    StructType(Array(
      StructField("feature_index", MapType(StringType, StringType)),
      StructField("day", StringType)
    ))
  }

  //   聚合结果类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("device_tm", MapType(StringType, types.ArrayType(StringType)))))
  }

  //   返回的数据类型
  override def dataType: DataType = {
    MapType(StringType, types.ArrayType(StringType))
  }

  override def deterministic: Boolean = true

  //   在聚合之前初始化结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new mutable.HashMap[String, Seq[String]]() {
      override def initialSize (): Int = 50
    }
  }

  //   map侧合并
  // profile_id -> (day, value)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var tmMap = buffer.getMap[String, Seq[String]](0)
    val featureIndex = input.getMap[String, String](0)
    val day = input.getString(1)

    featureIndex.foreach{ case (profileId, value) =>
      tmMap = updateTag(tmMap, profileId, day, value)
    }

    buffer.update(0, tmMap)
  }

  //   reduce测合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var m1 = buffer1.getMap[String, Seq[String]](0)
    val m2 = buffer2.getMap[String, Seq[String]](0)

    m2.foreach{ case (profileId, Seq(day, value)) =>
      m1 = updateTag(m1, profileId, day, value)
    }

    buffer1.update(0, m1)
  }

  def updateTag(m: scala.collection.Map[String, Seq[String]],
    profileId: String, day: String, value: String): scala.collection.Map[String, Seq[String]] = {
    if (m.contains(profileId)) {
      if (m(profileId).head < day) { // update value
        m.updated(profileId, Seq(day, value))
      } else {
        m
      }
    } else {
      m.updated(profileId, Seq(day, value))
    }
  }


  //   返回udaf最后的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getMap[String, Seq[String]](0)
  }
}