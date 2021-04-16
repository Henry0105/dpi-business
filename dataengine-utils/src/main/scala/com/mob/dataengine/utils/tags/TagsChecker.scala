package com.mob.dataengine.utils.tags

import com.mob.dataengine.rpc.RpcClient
import com.mob.dataengine.utils.DateUtils
import com.mob.dataengine.utils.tags.deps._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.collection.immutable

case class Param(
  tableName: String = "",
  zk: String = "",
  familyName: String = "c",
  rpcHost: String = "",
  rpcPort: Int = 0
) extends Serializable

case class TagsChecker(param: Param) extends Serializable {
  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .appName(s"device_tags_check[${DateUtils.currentDay()}]")
    .enableHiveSupport()
    .getOrCreate()

  @transient val cxt = Context(
    spark = spark,
    timestampHandler = null,
    tableStateManager = new AndroidTableStateManager(spark),
    sample = true,
    check = true
  )

  def sql(sql: String): DataFrame = {
    println("\n>>>>>>>>>>>>>>>>>")
    println(sql)
    val df = spark.sql(sql)
    println("<<<<<<<<<<<<<<\n\n")
    df
  }

  /**
   * 根据数据类型将数据反序列化
   */
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

  /**
   * 校验数据
   */
  def check(dataset: AbstractDataset): Unit = {
    val df: DataFrame = dataset.dataset.drop(dataset.timeCol)

    /* 获取当前DataFrame 列名称和类型 */
    val colNameDataTypesT = df.schema.toList
      .map(field => (field.name, field.dataType))
      .filter(col => !col._1.toLowerCase().equals("device"))
    val colNameDataTypesBC = spark.sparkContext.broadcast(colNameDataTypesT)


    df.repartition(1).rdd.foreachPartition(iterator => {
      /* hbase的rowkey在hive种的映射字段名称 */
      val rowKeyColName = "device"
      val colNameDataTypes: immutable.Seq[(String, DataType)] = colNameDataTypesBC.value
      val rows = iterator.toList
      /* 将DF中的device字段作为查询hbase的rowkey */
      val gets: List[Get] = rows.map(row => {
        val get = new Get(Bytes.toBytes(row.getAs[String](rowKeyColName)))
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
          colName: String, device: String,
          hiveValue: String, hbaseValue: String): Unit = {
          logger.warn(
            s"""
               |数据校验，发现不一致数据！
               |zk:$zk
               |hive_device:$device,
               |tagName:$colName,
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
               |tagName:$colNameDataTypes
               |total:$total
             """.stripMargin)

          val results: Array[Result] = hTable.get(gets.asJava)
          /* 比较查询结果 */
          results.zip(rows).foreach { case (result, row) =>
            var flag = false
            colNameDataTypes.foreach(colNameDataType => {
              val colName = colNameDataType._1
              val device = row.getAs[String](rowKeyColName)
              val hiveValueObject = row.getAs[Object](colName)
              val hiveValue = if (hiveValueObject != null) {
                hiveValueObject.toString
              } else {
                null
              }
              val hbaseValueByte = result.getValue(Bytes.toBytes(param.familyName), Bytes.toBytes(colName))
              val hbaseValue: String = if (hbaseValueByte != null) {
                getHbaseType(hbaseValueByte, colNameDataType._2)
              } else {
                null
              }
              if (hbaseValue != null && hiveValue != null && !hbaseValue.equals(hiveValue)) {
                if (!CheckUtil.checkStrategy(hbaseValue, hbaseValue, colNameDataType._2)) {
                  inconsistentData(zk, colNameDataType, colName, device, hiveValue, hbaseValue)
                  flag = true
                }
              } else if ((hbaseValue != null && hiveValue == null) || (hbaseValue == null && hiveValue != null)) {
                /* 增加特例null="" */
                if (!CheckUtil.checkNullAndEmpty(hbaseValue, hiveValue)) {
                  inconsistentData(zk, colNameDataType, colName, device, hiveValue, hbaseValue)
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
               |数据校验，发现不一致数据!
               |zk:$zk
               |tagName:$colNameDataTypes
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

  def run(): Unit = {
    val appPortrait = DmpAppPortrait(cxt)
    val dmpAppPortraitDF = appPortrait.dataset
    dmpAppPortraitDF.cache()
    import spark.implicits._
    // send sample to client
    val deviceSample = dmpAppPortraitDF.select($"device").take(100).map(row => row.getString(0))
    if (param.rpcPort != 0) {
      RpcClient.send(param.rpcHost, param.rpcPort, s"2\u0001${deviceSample.mkString("\u0002")}")
    }
    check(appPortrait)
    check(FinancialInstalled(cxt))
    check(DmpTourPortrait(cxt))
    check(DmpCookPortrait(cxt))
    check(DeviceProfileFull(cxt))
    check(DeviceTagCodIsFull(cxt))
  }

}


object TagsChecker {
  lazy private[this] val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val defaultParam: Param = Param()
    val projectName = s"BulkLoad[${DateUtils.currentDay()}]"
    val parser = new OptionParser[Param](projectName) {
      head(s"$projectName")
      opt[String]('t', "tableName")
        .text(s"hbase table name")
        .required()
        .action((x, c) => c.copy(tableName = x))
      opt[String]('z', "zk")
        .text(s"hbase zk集群组 ';'分割集群, 例如:zk1_1:2181,zk1_2:2181;zk2_1:2181,zk2_2:2181")
        .required()
        .action((x, c) => c.copy(zk = x))
      opt[Int]('p', "rpcPort")
        .text(s"thrift rpc port")
        .action((x, c) => c.copy(rpcPort = x))
      opt[String]('h', "rpcHost")
        .text(s"thrift rpc host")
        .action((x, c) => c.copy(rpcHost = x))
    }

    parser.parse(args, defaultParam) match {
      case Some(param) =>
        logger.info(param.toString)
        TagsChecker(param).run()
      case _ => sys.exit(1)
    }
  }
}

/**
 * 特列校验
 */
object CheckUtil {

  def checkStrategy(input2: String, input1: String, dataType: DataType): Boolean = {
    dataType match {
      case StringType => checkMappingGroup(input2, input1)
      case _ => false
    }
  }

  /**
   * 特例null=""
   */
  def checkNullAndEmpty(input2: String, input1: String): Boolean = {
    (input2 == null && input1 != null && input1.length == 0) || (input2 != null && input2.length == 0 && input1 == null)
  }

  /**
   * 校验乱序映射eg:(input1=aa|||bb|||cc=1|||2|||3,input2=aa|||bb|||c=1|||3|||2
   */
  def checkMappingGroup(input2: String, input1: String): Boolean = {
    if (input1.contains("|||") && input2.contains("|||") &&
      input1.contains("=") && input2.contains("=") && input1.length == input2.length) {
      var result = true
      val input1Map = input1.split("=").map(temp => temp.split("|||"))
      val input2Map = input2.split("=").map(temp => temp.split("|||"))
      if (input1Map.length == 2 && input2Map.length == 2) {
        val input1Tuple = input1Map(0).zip(input1Map(1)).sortBy(tuple => tuple._1)
        val input2Tuple = input2Map(0).zip(input2Map(1)).sortBy(tuple => tuple._1)
        input1Tuple.zip(input2Tuple).foreach(tuple => {
          if (!tuple._1._1.equals(tuple._2._1) || !tuple._1._2.equals(tuple._2._2)) {
            result = false
          }
        })
      } else {
        result = false
      }
      true
    } else {
      false
    }
  }
}

/**
 *
 * spark2-submit --executor-memory 10g  \
 *  --master yarn \
 *  --executor-cores 4  \
 *  --name TagsChecker[20180821]  \
 *  --deploy-mode cluster \
 *  --class com.mob.dataengine.utils.tags.TagsChecker \
 *  --driver-memory 4g  \
 *  --conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45" \
 *  --conf "spark.memory.storageFraction=0.1" \
 *  --conf "spark.speculation.quantile=0.99"  \
 *  --conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45" \
 *  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
 *  -XX:+PrintTenuringDistribution" \
 *  --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 *  --conf "spark.speculation=true" \
 *  --conf "spark.shuffle.service.enabled=true" \
 *   --files /home/dataengine/latest/distribution/conf/log4j.properties
 *   /home/dataengine/latest/distribution/conf/hive_database_table.properties \
 *   /home/dataengine/latest/distribution/lib/dataengine-utils-v0.7.2.1-jar-with-dependencies.jar  \
 *    --tableName rp_device_tags_info --zk bd15-098,bd15-099,bd15-107
 */
