package com.mob.dataengine.commons.traits

import org.apache.spark.sql.DataFrame

/**
 * @author juntao zhang
 */
trait Cacheable extends Watching {

  val debug: Boolean = false

  /**
   * 通过sql()函数立即进行表CACHE
   *
   * @param input     输入源DataFrame
   * @param tableName 临时表名
   * @return 源DataFrame
   */
  def cacheImmediately(input: DataFrame, tableName: String, checkpoint: Boolean = false): DataFrame = {
    watch(s"cacheImmediately-$tableName", () => {
      if (tableExists(tableName)) {
        dropTempView(tableName)
      }

      val output = if (checkpoint) {
        input.checkpoint()
      } else {
        input
      }

      output.createOrReplaceTempView(tableName)
      sql(s"CACHE TABLE $tableName")

      if (debug) {
        logger.info(
          s"""
             |${showString(output, 20, -1)}
             |count:${output.count()}
             |checkpoint:$checkpoint
           """.stripMargin)
      }
      spark.table(tableName)
    })
  }

  def uncacheTable(name: String): Unit = {
    if (spark.catalog.isCached(name)) {
      spark.table(name).unpersist()
    }
  }

  /**
   * 取消表缓存
   *
   * @param tableName 表名
   */
  protected def uncached(tableName: String): Unit = {
    if (spark.catalog.tableExists(tableName) && spark.catalog.isCached(tableName)) {
      spark.catalog.uncacheTable(tableName)
    }
  }

  def dropTempView(name: String): Unit = {
    if (!spark.catalog.tableExists(name)) {
      return
    }
    uncacheTable(name)
    spark.catalog.dropTempView(name)
  }

  def tableExists(name: String): Boolean = {
    spark.catalog.tableExists(name)
  }

  def tableNotExists(name: String): Boolean = {
    !tableExists(name)
  }
}
