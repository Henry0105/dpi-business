package com.mob.dataengine.commons

import org.apache.spark.sql.{DataFrame, SaveMode}

object HDFSWriter {
  def writeDataFrame(df: DataFrame, hdfsOutput: String, compression: Option[String] = None): Unit = {
    var writer = df.coalesce(1).write.mode(SaveMode.Overwrite)

    writer = compression match {
      case Some("tgz") => writer.option("compression", "gzip")
      case None => writer
    }

    writer.json(hdfsOutput)
  }
}
