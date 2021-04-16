package com.mob.dataengine.engine.core.other

import com.mob.dataengine.commons.utils.{Md5Helper, PropUtils}
import org.apache.spark.sql.SparkSession

object AllImeiNumbers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AllImeiNumbers").getOrCreate()

    import spark.implicits._
    val tmpTable = "tmp"
    val tmpTable1 = "tmp_1"
    val IMEI = "imei"

    val hexList = (0 to 15).map("%x".format(_))

    val prefix = for {
      x <- hexList
      y <- hexList
    } yield (x, y)

    spark.sparkContext.parallelize(prefix, prefix.size).mapPartitions{ iter =>
      iter.flatMap{ case (x, y) =>
        val imeiNumbers = (0 to 0xfffff).map { j =>
          val nums5 = "%05x".format(j)
          s"$x$y$nums5"
        }
        imeiNumbers
      }
    }.toDF(IMEI)
      .createOrReplaceTempView(tmpTable)

    spark.sparkContext.parallelize(prefix, prefix.size).mapPartitions{ iter =>
      iter.flatMap{ case (x, y) =>
        val imeiNumbers = (0 to 0xffffff).map { j =>
          val nums6 = "%06x".format(j)
          s"$x$y$nums6"
        }
        imeiNumbers
      }
    }.toDF(IMEI)
      .createOrReplaceTempView(tmpTable1)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_TOTAL_IMEI_MD5_MAPPING}
         |SELECT $IMEI, md5($IMEI)
         |FROM $tmpTable
         |UNION ALL
         |SELECT $IMEI, md5($IMEI)
         |FROM $tmpTable1
         """.stripMargin
    )

    spark.stop()
  }
}
