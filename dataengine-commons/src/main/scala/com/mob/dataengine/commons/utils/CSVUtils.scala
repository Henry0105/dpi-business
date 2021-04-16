package com.mob.dataengine.commons.utils

import java.io.{BufferedReader, File, InputStreamReader}

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, lit, split}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

object CSVUtils {
  def loadDataIntoHive(spark: SparkSession, _path: String, table: String, sep: String = ","): Unit = {
    val path = _path.replace("/", File.separator)
    var data = spark.read.option("sep", sep).option("header", "true").csv(path).toDF()

    val dataSchema = data.schema
    val tableSchema = spark.table(table).schema
    val partCols = getTablePartitionColumns(spark, table)

    tableSchema.foreach{ f =>
      if (!dataSchema.exists(dataField => dataField.name.equals(f.name))) {
        // 添加列
        val defaut = f.dataType match {
          case StringType => f.name
          case _ => null
        }
        data = data.withColumn(f.name, lit(defaut))
      } else if (f.dataType.isInstanceOf[ArrayType]) {
        data = data.withColumn(f.name, split(col(f.name), ","))
      }
    }

    val dataTable = "data_table"
    data.createOrReplaceTempView(dataTable)

    if (partCols.isEmpty) {
      spark.sql(
        s"""
          |insert overwrite table $table
          |select ${tableSchema.map(_.name).mkString(",")}
          |from $dataTable
        """.stripMargin)
    } else {
      spark.sql("set hive.exec.dynamic.partition=true")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      spark.sql(
        s"""
          |insert overwrite table $table partition(${partCols.mkString(",")})
          |select ${tableSchema.map(_.name).mkString(",")}
          |from $dataTable
        """.stripMargin)
    }
  }

  def getTablePartitionColumns(spark: SparkSession, table: String): Array[String] = {
    import spark.implicits._

    val cols = spark.sql(
      s"""
        |desc $table
      """.stripMargin)
      .map(_.getString(0)).collect()
    val idx = cols.indexOf("# col_name")
    if (idx < 0) {
      Array.empty[String]
    } else {
      cols.zipWithIndex.filter { case (_, i) => i > idx }.map(_._1)
    }
  }

  def fromCsvIntoHive(_csvPath: String, spark: SparkSession, tableName: String,
    partition: String*): Unit = {
    val csvPath = _csvPath.replace("/", File.separator)
    val reader = new BufferedReader(
      new InputStreamReader(this.getClass.getClassLoader.getResourceAsStream(csvPath),
        "utf-8"
      )
    )

    val schema = spark.table(tableName).schema
    val fieldNames = schema.fieldNames
    val typeNames = schema.fields

    val values =
      new CSVParser(reader, CSVFormat.EXCEL.withHeader()).iterator().asScala.toSeq.map(
        r => {
          val valueArr = scala.collection.mutable.ArrayBuffer[String]()
          val fieldArr = scala.collection.mutable.ArrayBuffer[String]()
          val partitionArr = scala.collection.mutable.ArrayBuffer[String]()

          for(i <- 0 until  fieldNames.size) {
            val field = fieldNames(i)
            try {
              valueArr += r.get(field)
              // println(field + "----" + typeNames(i).dataType.typeName)
              if (!partition.contains(field)) {
                fieldArr += getFiled(field, typeNames(i).dataType.typeName)
              } else {
//                partitionArr += getPartition(field, r.get(field), typeNames(i).dataType.typeName)
                partitionArr += field
              }
            } catch {
              case e: IllegalArgumentException =>
                valueArr += r.get("\uFEFF" + field)
                if (!partition.contains(field)) {
                  fieldArr += getFiled(field, typeNames(i).dataType.typeName)
                } else {
                  // \uFEFF代表CSV开头的一个符号，文件自带的
//                  partitionArr += getPartition(field, r.get("\uFEFF" + field), typeNames(i).dataType.typeName)
                  partitionArr += field
                }
            }
          }
          (Row.fromSeq(valueArr), fieldArr, partitionArr)
        }
      )

    val schema2 = StructType(
      fieldNames.map(field => {
        StructField(field.replaceAll("\uFEFF", "").trim, StringType, nullable = true)
      })
    )

    if(partition.nonEmpty) {
      spark.createDataFrame(values.map(_._1).asJava, schema2).createOrReplaceTempView(viewName = "t")
      insertintoHive(spark, tableName, values(0)._2, values(0)._3)
    }
    else {
      spark.createDataFrame(values.map(_._1).asJava, schema2).createOrReplaceTempView(viewName = "t")
      insertintoHive(spark, tableName, values(0)._2)
    }
  }

  private def insertintoHive(spark: SparkSession, table: String, field: Seq[String], partition: Seq[String]): Unit = {
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE $table PARTITION (${partition.mkString(",")})
         |SELECT ${field.mkString(",")},${partition.mkString(",")}
         |FROM t
       """.stripMargin)
  }

  private def insertintoHive(spark: SparkSession, table: String, field: Seq[String]): Unit = {
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE $table
         |SELECT ${field.mkString(",")}
         |FROM t
       """.stripMargin)
  }

  def getFiled(fieldName: String, typeName: String): String = {
    typeName match {
      case "string" => fieldName
      case "integer" => s" cast ($fieldName as int)"
      case "long" => s"cast ($fieldName as bigint)"
      case "double" => s"cast ($fieldName as double)"
      case _ => null
    }
  }

  def getPartition(partition: String, value: String, typeName: String): String = {
    typeName match {
      case "string" => s"$partition = '$value'"
      case "integer" => s"$partition = $value"
      case "long" => s"$partition = $value"
      case "double" => s"$partition = $value"
      case _ => null
    }
  }
}
