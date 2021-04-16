package org.apache.spark.sql.hive.orc

import java.io.File

import com.mob.dataengine.commons.traits.Logging
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.collection.mutable

/**
 * @author juntao zhang
 */
case class OrcIndexWriter(
  @transient spark: SparkSession,
  table: String,
  warehouse: String = s"${File.separator}user${File.separator}hive${File.separator}warehouse"
) extends Logging {

  import spark.implicits._

  spark.udf.register("agg_fn", new AggFunction())
  spark.udf.register("map_agg_fn", new MapAggFunction())

  def write(day: String): Unit = {
    val Array(db, tableName) = table.split("\\.")
    val dir = s"$warehouse${File.separator}$db.db${File.separator}$tableName${File.separator}day=$day"
    val conf = spark.sparkContext.hadoopConfiguration

    val orcFile = spark.sparkContext.newAPIHadoopFile(
      dir,
      classOf[OrcIndexInputFormat],
      classOf[NullWritable],
      classOf[Text],
      conf
    )

    logger.info(
      s"""
         |
         |partitions:
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |${orcFile.partitions.mkString("\n")}
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |
      """.stripMargin)

    orcFile.map(_._2.toString).map { str =>
      val Array(device, rowNumber, fileName) = str.split("\u0001")
      (device, fileName, rowNumber.toInt)
    }.toDF(
      "device", "file_name", "row_number"
    ).repartition(128).createOrReplaceTempView("t")

    sql(
      s"""
         |insert overwrite table rp_mobdi_app.timewindow_online_profile_day_index partition(table='$table',day='$day')
         |select device,file_name,row_number from t
      """.stripMargin)
  }

  def aggregate(start: String, end: String, version: String): Unit = {
    sql(
      s"""
         |insert overwrite table rp_mobdi_app.profile_history_index partition(table='$table',version='$version')
         |select device,agg_fn(file_name,row_number,day) as feature_index
         |from rp_mobdi_app.timewindow_online_profile_day_index
         |where day>=$start and day<=$end
         |group by device
      """.stripMargin)
  }

  def aggregate(versions: Seq[String], newVersion: String): Unit = {
    sql(
      s"""
         |insert overwrite table rp_mobdi_app.profile_history_index partition(table='$table',version='$newVersion')
         |select device,map_agg_fn(feature_index) as feature_index
         |  from rp_mobdi_app.profile_history_index
         |  where version in (${versions.map(v => s"'$v'").mkString(",")}) and table='$table'
         |group by device
      """.stripMargin)
  }

  def daysBetween(start: String, end: String): Set[String] = {
    val s = DateTime.parse(start, DateTimeFormat.forPattern("yyyyMMdd"))
    val e = DateTime.parse(end, DateTimeFormat.forPattern("yyyyMMdd"))
    val days = Days.daysBetween(s, e).getDays
    (0 to days).map(s.plusDays).map(t => t.toString("yyyyMMdd")).toSet
  }

  def update(start: String, end: String, version: String, newVersion: String): Unit = {
    val filters = daysBetween(start, end)

    def cleanFn: UDF1[Map[String, Row], Map[String, Row]] = new UDF1[Map[String, Row], Map[String, Row]] {
      override def call(fi: Map[String, Row]): Map[String, Row] = {
        fi.filter(p => !filters.contains(p._1))
      }
    }

    val schema = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    spark.udf.register("clean_fn", cleanFn, schema)
    sql(
      s"""
         |insert overwrite table rp_mobdi_app.profile_history_index partition(table='$table',version='$newVersion')
         |select device, map_agg_fn(feature_index) as feature_index
         |from (
         | select device,agg_fn(file_name,row_number,day) as feature_index
         | from rp_mobdi_app.timewindow_online_profile_day_index
         | where day>=$start and day<=$end and table='$table'
         | group by device
         |
         | union all
         |
         | select device,clean_fn(feature_index) as feature_index
         | from rp_mobdi_app.profile_history_index
         | where version='$version' and table='$table'
         |) t
         |group by device
      """.stripMargin)
  }

  class AggFunction() extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      StructType(Array(
        StructField("file_name", StringType),
        StructField("row_number", LongType),
        StructField("day", StringType)
      ))
    }

    override def bufferSchema: StructType = new StructType().add(
      StructField("t", MapType(StringType, StructType(Array(
        StructField("file_name", StringType),
        StructField("row_number", LongType)
      ))))
    )

    override def dataType: MapType = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, mutable.HashMap[String, Row]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val m = buffer.getMap[String, Row](0)
      val fileName = input.getString(0)
      val num = input.getLong(1)
      val day = input.getString(2)
      buffer.update(0, m.updated(day, Row(fileName, num)))
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val m1 = buffer1.getMap[String, Row](0)
      val m2 = buffer2.getMap[String, Row](0)
      buffer1.update(0, m1 ++ m2)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getMap[String, Row](0)
    }
  }

  class MapAggFunction() extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = new StructType().add(
      StructField("feature_index", MapType(StringType, StructType(Array(
        StructField("file_name", StringType),
        StructField("row_number", LongType)
      ))))
    )

    override def bufferSchema: StructType = new StructType().add(
      StructField("t", MapType(StringType, StructType(Array(
        StructField("file_name", StringType),
        StructField("row_number", LongType)
      ))))
    )

    override def dataType: MapType = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, mutable.HashMap[String, Row]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val m1 = buffer.getMap[String, Row](0)
      val m2 = input.getMap[String, Row](0)
      buffer.update(0, m1 ++ m2)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val m1 = buffer1.getMap[String, Row](0)
      val m2 = buffer2.getMap[String, Row](0)
      buffer1.update(0, m1 ++ m2)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getMap[String, Row](0)
    }
  }

}

object OrcIndexWriter {
  def main(args: Array[String]): Unit = {
    val Array(start, end) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index")
      .enableHiveSupport()
      .getOrCreate()
    val writer = new OrcIndexWriter(spark, "rp_mobdi_app.timewindow_online_profile_day")

    writer.daysBetween(start, end).toList.sorted.foreach { day =>
      println(s"day=>$day ...")
      writer.write(day)
      println(s"day=>$day finished\n\n\n")
    }
  }
}

object OrcIndexAggregator {
  def main(args: Array[String]): Unit = {
    println(args)
    val Array(start, end, version) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index")
      .enableHiveSupport()
      .getOrCreate()
    new OrcIndexWriter(spark, "rp_mobdi_app.timewindow_online_profile_day").aggregate(
      start, end, version
    )
  }
}

object OrcIndexAggregator2 {
  def main(args: Array[String]): Unit = {
    println(args)
    val Array(versions, newVersion) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index")
      .enableHiveSupport()
      .getOrCreate()
    new OrcIndexWriter(spark, "rp_mobdi_app.timewindow_online_profile_day").aggregate(
      versions.split(","), newVersion
    )
  }
}

object OrcIndexUpdater {
  def main(args: Array[String]): Unit = {
    val Array(start, end, version, newVersion) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index")
      .enableHiveSupport()
      .getOrCreate()
    new OrcIndexWriter(spark, "rp_mobdi_app.timewindow_online_profile_day").update(
      start, end, version, newVersion
    )
  }
}
