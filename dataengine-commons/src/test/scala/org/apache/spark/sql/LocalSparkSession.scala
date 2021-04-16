package org.apache.spark.sql

import java.io.File

import com.mob.dataengine.commons.traits.{Logging, TableTrait}
import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.utils.FileUtils
import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
 * Manages a local `spark` {@link SparkSession} variable, correctly stopping it after each test.
 *
 * @author juntao zhang
 */
trait LocalSparkSession extends BeforeAndAfterEach with BeforeAndAfterAll with TableTrait {
  self: Suite =>

  @transient val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.warehouse.dir", makeWarehouseDir().toURI.getPath)
    // SPARK-8910
    .config("spark.ui.enabled", "false")
    .appName("test").master("local[1]").enableHiveSupport().getOrCreate()

  def makeWarehouseDir(): File = {
    val warehouseDir = Utils.createTempDir(namePrefix = "warehouse")
    warehouseDir.delete()
    warehouseDir
  }

  override def beforeAll() {
    super.beforeAll()
    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)
  }

  def createTable(schema: String): Unit = {
    sql(schema)
  }

  def deleteHdfsPath(path: String): Unit = {
    import org.apache.hadoop.fs.{FileSystem, Path}

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outPutPath = new Path(path)
    if (fs.exists(outPutPath)) {
      fs.delete(outPutPath, true)
    }
  }

  //  override def afterEach() {
  //    try {
  //      resetSparkContext()
  //    } finally {
  //      super.afterEach()
  //    }
  //  }
  //
  //  def resetSparkContext(): Unit = {
  //    LocalSparkSession.stop(spark)
  //    spark = null
  //  }

  def stop(): Unit = {
    LocalSparkSession.stop(spark)
  }

  def createView(table: String, view: String): Unit = {
    val cols = spark.table(table).schema.fieldNames
    spark.sql(
      s"""
         |CREATE OR REPLACE VIEW $view AS
         |SELECT ${cols.mkString(",")}
         |FROM $table
         |""".stripMargin
    )
  }

  /**
   * @param part: 分区信息, eg: day=''20190101''
   * @param excludePartCols: 是否在insert语句中去掉分区字段
   */
  def insertDF2Table(df: DataFrame, table: String, part: Option[String] = None,
    excludePartCols: Boolean = true): Unit = {
    df.createOrReplaceTempView("tmp")
    val dfCols: Seq[String] = df.schema.map(_.name)
    val tableCols = spark.table(table).schema.map(_.name)
    val filteredCols: Seq[String] = if (excludePartCols) {
      val partCols = CSVUtils.getTablePartitionColumns(spark, table)
      tableCols.diff(partCols)
    } else {
      tableCols
    }

    val selectClause = filteredCols.map{ c =>
      if (dfCols.contains(c)) {
        c
      } else {
        s"null as $c"
      }
    }.mkString(",")

    sql(
      s"""
         |insert overwrite table $table ${if (part.nonEmpty) s"partition(${part.get})" else ""}
         |select $selectClause
         |from tmp
       """.stripMargin)
  }

  def prepareCodeMapping(): Unit = {
    CSVUtils.loadDataIntoHive(spark, "../data/mapping/dm_dataengine_code_mapping.csv",
      PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
  }
}

object LocalSparkSession {
  def stop(spark: SparkSession) {
    if (spark != null) {
      spark.stop()
    }
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSparkSession[T](sc: SparkSession)(f: SparkSession => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}

