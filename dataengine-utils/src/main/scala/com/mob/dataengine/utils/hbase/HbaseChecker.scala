package com.mob.dataengine.utils.hbase

import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class Param(
  tableName: String = "",
  zk: String = "",
  familyName: String = "c",
  rowKeyColName: String = "device",
  rpcHost: String = "",
  rpcPort: Int = 0
)

abstract class HbaseChecker(spark: SparkSession, param: Param) extends Serializable {

  import com.mob.dataengine.utils.tags.CheckUtil

  /**
   * 校验数据
   */
  def check(df: DataFrame, isRowKeyHexed: Boolean = true): Unit = {
    /* 获取当前DataFrame 列名称和类型 */
    val colNameDataTypesT = df.schema.toList
      .map(field => (field.name, field.dataType))
      .filter(col => !col._1.toLowerCase().equals(param.rowKeyColName))
    val colNameDataTypesBC = spark.sparkContext.broadcast(colNameDataTypesT)

    df.repartition(1).rdd.foreachPartition(iterator => {
      /* hbase的rowkey在hive种的映射字段名称 */
      val colNameDataTypes: Seq[(String, DataType)] = colNameDataTypesBC.value
      val rows = iterator.toList
      /* 将DF中的id字段作为查询hbase的rowkey */
      val gets: List[Get] = rows.map(row => {
        val get = if (isRowKeyHexed) {
          new Get(Hex.decodeHex(row.getAs[String](param.rowKeyColName).toCharArray))
        } else {
          new Get(Bytes.toBytes(row.getAs[String](param.rowKeyColName)))
        }
        colNameDataTypes.foreach(colName_dataType => {
          get.addColumn(Bytes.toBytes(param.familyName), Bytes.toBytes(colName_dataType._1))
        })
        get
      })
      val logger = LoggerFactory.getLogger(getClass)

      def checkHbase(zk: String): Unit = {
        val total = rows.length
        var diff = 0

        /* 在work端校验数据 */
        def inconsistentData(
          zk: String,
          colNameDataType: (String, DataType),
          colName: String, id: String,
          hiveValue: String, hbaseValue: String): Unit = {
          logger.warn(
            s"""
               |数据校验，发现不一致数据！
               |zk:$zk
               |hive_id:$id,
               |colName:$colName,
               |hiveValue:$hiveValue,
               |hbaseValue:$hbaseValue,
               |data_type:${colNameDataType._2}
             """.stripMargin)
        }

        /* 获取hbase连接 */
        var conn: Connection = null
        var hTable: Table = null
        try {
          val conf: Configuration = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", zk)
          conn = ConnectionFactory.createConnection(conf)
          hTable = conn.getTable(TableName.valueOf(param.tableName))

          logger.info(
            s"""
               |zk:$zk
               |colName:$colNameDataTypes
               |total:$total
             """.stripMargin)

          val results: Array[Result] = hTable.get(gets.asJava)
          /* 比较查询结果 */
          results.zip(rows).foreach { case (result, row) =>
            var flag = false
            colNameDataTypes.foreach(colNameDataType => {
              val colName = colNameDataType._1
              val id = row.getAs[String](param.rowKeyColName)
              val hiveValueObject = row.getAs[Object](colName)
              val hiveValue = if (hiveValueObject != null) {
                hiveValueObject.toString
              } else {
                null
              }
              val hbaseValueByte = result.getValue(Bytes.toBytes(param.familyName), Bytes.toBytes(colName))
              val hbaseValue: String = if (hbaseValueByte != null) {
                HbaseChecker.getHbaseType(hbaseValueByte, colNameDataType._2)
              } else {
                null
              }
              if (hbaseValue != null && hiveValue != null && !hbaseValue.equals(hiveValue)) {
                if (!CheckUtil.checkStrategy(hbaseValue, hbaseValue, colNameDataType._2)) {
                  inconsistentData(zk, colNameDataType, colName, id, hiveValue, hbaseValue)
                  flag = true
                }
              } else if ((hbaseValue != null && hiveValue == null) || (hbaseValue == null && hiveValue != null)) {
                /* 增加特例null="" */
                if (!CheckUtil.checkNullAndEmpty(hbaseValue, hiveValue)) {
                  inconsistentData(zk, colNameDataType, colName, id, hiveValue, hbaseValue)
                  flag = true
                }
              }
            })
            if (flag) {
              diff += 1
            }
          }
        } finally {
          if (hTable != null) {
            hTable.close()
          }
          if (conn != null && !conn.isClosed) {
            hTable.close()
          }
        }
        if ((diff / (total * 1.0)) > 0.2) {
          throw new Exception(
            s"""
               |数据校验，发现不一致数据！
               |zk:$zk
               |colName:$colNameDataTypes
               |diff:$diff
               |total:$total
               |diffRate:${diff / (total * 1.0)}
               |""".stripMargin
          )
        }
      }

      param.zk.split(";").foreach(checkHbase)

    })
  }
}

object HbaseChecker {
  def getHbaseType(byte: Array[Byte], coltype: DataType): String = {
    coltype match {
      case StringType => Bytes.toString(byte)
      case IntegerType => Bytes.toInt(byte).toString
      case DoubleType => Bytes.toDouble(byte).toString
      case FloatType => Bytes.toFloat(byte).toString
      case BooleanType => Bytes.toBoolean(byte).toString
      case LongType => Bytes.toLong(byte).toString
      case ShortType => Bytes.toShort(byte).toString
      case _ => Bytes.toString(byte)
    }
  }
}
