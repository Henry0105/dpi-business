package com.mob.dataengine.engine.core.other

import com.mob.dataengine.commons.utils.{Md5Helper, PropUtils}
import org.apache.spark.sql.SparkSession

object AllMacNumbers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AllMacNumbers").getOrCreate()

    import spark.implicits._
    val tmpTable = "tmp"
    val MAC = "mac"

    val prefix = (0 to 15).map("%x".format(_))

    spark.sparkContext.parallelize(prefix, prefix.size).mapPartitions{ iter =>
      iter.flatMap{ x =>
        val macNumbers = (0 to 0xfffff).map { j =>
          val nums5 = "%05x".format(j)
          s"$x$nums5"
        }
        macNumbers
      }
    }.toDF(MAC)
      .createOrReplaceTempView(tmpTable)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_TOTAL_MAC_MD5_MAPPING}
         |SELECT $MAC, md5($MAC)
         |FROM $tmpTable
         """.stripMargin
    )

    spark.stop()
  }
}
