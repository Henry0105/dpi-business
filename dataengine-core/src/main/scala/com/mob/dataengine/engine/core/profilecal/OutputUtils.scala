package com.mob.dataengine.engine.core.profilecal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
 * 输出工具类
 */
object OutputUtils {

  /**
   * HDFS文件导出
   *
   * @param hc         HiveContext
   * @param sql        导出的sql
   * @param outputPath 导出hdfs路径
   */
  def doExport(hc: SparkSession, sql: String, outputPath: String, outputBlocksNum: Int): Unit = {
    val data = hc.sql(sql)
    val fieldLength = data.schema.fieldNames.length
    val fs = FileSystem.get(hc.sparkContext.hadoopConfiguration)
    val outPath = new Path(outputPath)
    if (fs.exists(outPath)) {
      fs.delete(outPath, true)
    }
    data.rdd.map(row => {
      var result = ""
      var i = 0
      while (i < fieldLength) {
        result += s"${row.get(i)}${ProfileConstants.EXPORT_DATA_SEPARATOR}"
        i += 1
      }
      result
    }).coalesce(outputBlocksNum).saveAsTextFile(outputPath)
  }

}
