package com.mob.dataengine.engine.core.other

import com.mob.dataengine.commons.utils.{Md5Helper, PropUtils}
import org.apache.spark.sql.SparkSession

object AllPhoneNumbers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AllPhoneNumbers").getOrCreate()

    import spark.implicits._
    val tmpTable = "tmp"
    val PHONE = "phone"

    val prefix = for {
      x <- 3 to 9
      y <- 0 to 9
      z <- 0 to 9
    } yield (x, y, z)

    spark.sparkContext.parallelize(prefix, prefix.size).mapPartitions{ iter =>
      iter.flatMap{ case (x, y, z) =>
        val phoneNumbers = (0 to 9999999).map { j =>
          val nums9 = "%07d".format(j)
          s"1$x$y$z$nums9"
        }
        phoneNumbers
      }
    }.toDF(PHONE)
      .createOrReplaceTempView(tmpTable)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_TOTAL_PHONE_MD5_MAPPING}
         |SELECT $PHONE, md5($PHONE)
         |FROM $tmpTable
         """.stripMargin
    )

    spark.stop()
  }
}
