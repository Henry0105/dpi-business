package com.mob.dataengine.commons.traits

import java.io.PrintWriter

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.meta.getter

/**
 * @author juntao zhang
 */
trait Logging {
  protected var sqlNo = 1

  @transient protected[this] val logger: Logger = Logger.getLogger(this.getClass)

  @(transient@getter) protected val spark: SparkSession

  def sql(sqlString: String): DataFrame = {
    logger.info(
      s"""
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |Job[${this.getClass.getSimpleName}].SQL[No$sqlNo.]
         |
         |$sqlString
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
       """.stripMargin
    )
    sqlNo += 1
    spark.sql(sqlString)
  }

  def stackTraceToString(e: Exception): String = {
    import java.io.CharArrayWriter
    val caw = new CharArrayWriter
    val pw = new PrintWriter(caw)
    e.printStackTrace(pw)
    val result = caw.toString
    caw.close()
    pw.close()
    result
  }

  def showString(dataset: DataFrame, _numRows: Int = 20, truncate: Int = 20): String = {
    val numRows = _numRows.max(0)
    val takeResult = dataset.take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    val rows: Seq[Seq[String]] = dataset.schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }

    val sb = new StringBuilder
    val numCols = dataset.schema.fieldNames.length

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    rows.head.zipWithIndex.map { case (cell, i) =>
      if (truncate > 0) {
        StringUtils.leftPad(cell, colWidths(i))
      } else {
        StringUtils.rightPad(cell, colWidths(i))
      }
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map { case (cell, i) =>
        if (truncate > 0) {
          StringUtils.leftPad(cell.toString, colWidths(i))
        } else {
          StringUtils.rightPad(cell.toString, colWidths(i))
        }
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }

}
