package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.tags.{MaxAccumulator, TableStateManager, TimestampHandler}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

case class Context(
  @transient spark: SparkSession,
  tableStateManager: TableStateManager,
  timestampHandler: TimestampHandler,
  sample: Boolean = false,
  check: Boolean = false // 是否是检查逻辑
) extends Serializable {
  def tablePartitions: Map[String, String] = {
    if (check) {
      tableStateManager.lastTableStates
    } else {
      tableStateManager.tablePartitions
    }
  }

  def update(table: String, timestamp: String): Unit = {
    if (!check) tableStateManager.update(table, timestamp)
  }
}

abstract class AbstractDataset(cxt: Context) extends Serializable {

  val datasetId: String
  lazy val lastTsOpt: Option[String] = cxt.tableStateManager.lastTableStates.get(datasetId)

  @transient lazy val dataset: DataFrame = {
    val df = if (cxt.sample) {
      _dataset().limit(limitNum)
    } else {
      _dataset()
    }

    df.filter(r => {
      StringUtils.isNotBlank(r.getString(0)) && r.getString(0).matches("[a-f0-9]{40}")
    })
    /** .withColumn("device_par", devicePartition(df.col("device"))).repartition(
     * 1024, df.col("device_par"))
     */
    /** .sortWithinPartitions(df.col("deivce")).rdd.mapPartitions(new UniqueIterable(_)
     * )
     */
  }

  def timeCol: Column = dataset.col("day")

  /* 原始过滤，取原始数据的1/splitDataNum */
  val splitDataNum = 100000
  /* 随机取数据的hash(device)=randomType */
  val randomType: Int = (Math.random() * splitDataNum).toInt
  /* 最终校验数据的条数 */
  val limitNum = 1000

  // 1024
  lazy val span: Int = 0xff / 4 + 1

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ =>
      val a = key.asInstanceOf[Array[Byte]]
      (a(0) & 0xff) * 4 + (a(1) & 0xff) / span
  }

  def devicePartition: UserDefinedFunction = functions.udf((device: String) => {
    getPartition(Hex.decodeHex(device.toCharArray))
  })

  def processTs(currentTsOpt: Option[String]): Option[String] = {
    cxt.timestampHandler.processTs(
      currentTsOpt,
      lastTsOpt
    )
  }

  def sql(sqlString: String): DataFrame = {
    println("\n>>>>>>>>>>>>>>>>>")
    println(sqlString)
    val df = cxt.spark.sql(sqlString)
    println("<<<<<<<<<<<<<<\n\n")
    df
  }

  def sampleClause(key: String = "device"): String = {
    if (cxt.sample) {
      s"hash($key)%1000000=$randomType"
    } else {
      "true"
    }
  }

  def _dataset(): DataFrame

  def maxAccumulator(name: String): MaxAccumulator = {
    val acc = new MaxAccumulator
    cxt.spark.sparkContext.register(acc, name)
    acc
  }

  def onlineProfile2FinanceAdapter(feature2FieldMap: Map[String, String], tableName: String,
    day: String, flag: Int, tw: Int, isOnline: Boolean = true): Unit = {
    val featureFilter = feature2FieldMap.keys.map(s => s"'$s'").mkString(", ")
    val span = feature2FieldMap
      .map{ case (feature, field) => s"if(feature='$feature', cnt, 0) as $field" }
      .mkString(",")
    val selectFields = feature2FieldMap.values.map(f => s"max($f) as $f").mkString(",")

    val srcTable = if (isOnline) {
      PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2
    } else {
      PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2
    }

    sql(
      s"""
         |select device, '$day' as day, $selectFields
         |from (
         |  select device, $span
         |  FROM  $srcTable
         |  WHERE day = '$day' and ${sampleClause()} and feature in ( $featureFilter ) and
         |    flag = $flag and timewindow='$tw'
         |) as a
         |group by device
      """.stripMargin).createOrReplaceTempView(tableName)
  }
}

abstract class IosAbstractDataset(cxt: Context) extends AbstractDataset(cxt) with Serializable {
  @transient override lazy val dataset: DataFrame = {
    val df = if (cxt.sample) {
      _dataset().limit(limitNum)
    } else {
      _dataset()
    }

    df.filter(r => {
      StringUtils.isNotBlank(r.getString(0))
    })
  }
}
