package org.apache.spark.sql.hive.orc

import java.io.File

import com.mob.dataengine.commons.traits.Logging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, OrcStruct}
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoUtils}
import org.apache.hadoop.io.Text
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{HadoopUtils, Partitioner}
import org.apache.spark.sql.functions.count

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * todo zhangjt 需要更加通用
 * input "device", "day", "file", "row_number", "data"
 * output "device", "day", "features", "data"
 *
 * @author juntao zhang
 */
case class OrcIndexReader(
  @transient spark: SparkSession,
  @transient indexDF: DataFrame,
  table: String,
  warehouse: Option[String] = None
) extends Logging {

  import spark.implicits._

  def search(profileIds: Set[String]): DataFrame = {

    val Array(db, tableName) = table.split("\\.")
    val dir = s"${warehouse.getOrElse(s"/user/hive/warehouse")}/$db.db/$tableName"
    val fileIndex = indexDF.select("file").distinct().collect().map(_.getString(0)).zipWithIndex
    val file2IndexBC = spark.sparkContext.broadcast(
      fileIndex.toMap
    )

    println("文件行数统计:")
    indexDF.select("file").groupBy("file").agg(count("file").as("cnt"))
        .map(r => (r.getString(0), r.getLong(1))).collect()
        .foreach(println)

    println("所有的文件和分配的index:")
    fileIndex.sorted.foreach(println)

    logger.info(
      s"""
         |file2IndexBC=>
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |${fileIndex.sorted}
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
      """.stripMargin)

    val index2FileDF = spark.sparkContext.broadcast(
      fileIndex.map { case (i, v) => (v, i) }.toMap
    )

    val dataSchema = {
      StructType(Array(
        StructField("device", StringType),
        StructField("profile", MapType(StringType, StringType))
      ))
    }
    val hadoopConfig = HadoopUtils.broadcastHadoopConfiguration(spark)
    val splitsNum = fileIndex.length
    val threshold = 30
    indexDF.map { r =>
      (r.getAs[String]("file"),
        (r.getAs[String]("old_day"), r.getAs[String]("day"),
          r.getAs[Long]("row_number"), r.getAs[String]("data")))
    }.rdd.partitionBy(new Partitioner {
      override def numPartitions: Int = if (splitsNum % threshold == 0) splitsNum / threshold
      else splitsNum / threshold + 1

      override def getPartition(key: Any): Int = {
        file2IndexBC.value(key.asInstanceOf[String]) / threshold
      }
    }).mapPartitionsWithIndex { case (index, iter) =>
      iter.toList.groupBy(_._1).flatMap{ case (file, seq) =>
        try {
          val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(dataSchema.catalogString)
          val structOI = OrcStruct.createObjectInspector(typeInfo.asInstanceOf[StructTypeInfo])
            .asInstanceOf[SettableStructObjectInspector]
          val fileDay = file.split("day=")(1).split("/")(0)
          val reader = OrcFile.createReader(new Path(s"$dir/$file"), OrcFile.readerOptions(hadoopConfig.value.value))
          val recordReader = reader.rows()
          val orcStruct = structOI.create().asInstanceOf[OrcStruct]

          val tmp =
            seq.map(_._2).sortBy(-_._3).map { case (oldDay, day, rowNumber, data) =>
            recordReader.seekToRow(rowNumber)
            recordReader.next(orcStruct)
            val device = structOI.getStructFieldData(orcStruct, structOI.getStructFieldRef("device"))
              .asInstanceOf[Text].toString
            val features: mutable.Map[String, Array[String]] =
              structOI.getStructFieldData(orcStruct, structOI.getStructFieldRef("profile"))
                .asInstanceOf[java.util.Map[Text, Text]].asScala.map { case (profileId, v) =>
                profileId.toString -> Array(fileDay, if (v == null) null else v.toString)
            }.filter(kv => kv._2 != null && profileIds.contains(kv._1) && kv._2(1) != null)
            (device, oldDay, day, features, data)
          }.filter(_._4.nonEmpty)
          tmp
        } catch {
          case ex: Throwable => ex.printStackTrace(System.out)
            println(file, seq.map(_._2).map{ case (oldDay, day, rowNumber, data) =>
              s"$oldDay, $day, $rowNumber, $data"})
            Seq(("", "", "", mutable.Map.empty[String, Array[String]], ""))
        }
      }.filter(_._4.nonEmpty).iterator
    }.toDF("device", "old_day", "day", "features", "data")
  }
}
