package com.mob.dataengine.utils.tools

import java.net.URL
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.slf4j.{Logger, LoggerFactory}

/**
 * @param jobName           任务名称
 * @param bootstrapServer   kafka bootstrap-server
 * @param topic             kafka 主题
 * @param numPartitions     并发数,kafka生产者个数
 * @param kafkaProducerConf kafka生产者配置
 * @param sql               sql语句
 * @param udfs              udf别名及对应的全路径类名
 * @param udfJars           udf的jar包路径
 */
case class Hive2KafkaParams(
                             jobName: String = "",
                             bootstrapServer: String = "",
                             topic: String = "",
                             numPartitions: Int = 3,
                             kafkaProducerConf: Map[String, Object] = Map(),
                             sql: String = "",
                             udfs: Map[String, String] = Map(),
                             udfJars: Seq[String] = Seq()
                           ) {
  override def toString: String = {
    s"""
       |jobName = $jobName, bootstrap = $bootstrapServer, topic = $topic, numPartitions = $numPartitions,
       |kafkaProducerConf = $kafkaProducerConf,
       |sql = $sql,
       |udfs = $udfs, udfJars = $udfJars,
       |""".stripMargin
  }
}

object Hive2Kafka {
  @transient val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val header = "Hive2Kafka"
    val parser = new scopt.OptionParser[Hive2KafkaParams](header) {
      head(header)
      opt[String]("jobName")
        .text("任务名称")
        .action((x, c) => c.copy(jobName = x))

      opt[String]("topic")
        .required()
        .text("kafka topic")
        .action((x, c) => c.copy(topic = x))

      opt[String]("bootstrapServer")
        .required()
        .text("kafka bootstrap-server")
        .action((x, c) => c.copy(bootstrapServer = x))

      opt[Int]("numPartitions")
        .text("kafka 生产者个数")
        .action((x, c) => c.copy(numPartitions = x))

      opt[Map[String, String]]("kafkaProducerConf")
        .valueName("k1=v1,k2=v2...")
        .text("kafka 生产者的配置信息")
        .action((x, c) => c.copy(kafkaProducerConf = x))

      opt[String]("sql")
        .required()
        .text("sql")
        .action((x, c) => c.copy(sql = x))

      opt[Map[String, String]]("udfs")
        .valueName("k1=v1,k2=v2...")
        .text("udf别名及对应的全路径类名")
        .action((x, c) => c.copy(udfs = x))

      opt[Seq[String]]("udfJars")
        .valueName("<jar1>,<jar2>...")
        .text(s"udf的jar包路径")
        .action((x, c) => c.copy(udfJars = x))
    }

    val defaultParams = Hive2KafkaParams()

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        logger.info(s"任务参数: ${params.toString}")
        val spark: SparkSession = SparkSession
          .builder()
          .appName(s"Hive2Kafka_${params.jobName}")
          .enableHiveSupport()
          .getOrCreate()

        val gen = new Hive2Kafka(spark, params)
        gen.run()
        spark.stop()
      case _ =>
        logger.error(s"参数不对:${args.mkString(",")}")
        sys.exit(1)
    }
  }
}

class Hive2Kafka(@transient spark: SparkSession, p: Hive2KafkaParams) extends Serializable {
  @transient val logger: Logger = LoggerFactory.getLogger(getClass)
  val startTime: Long = System.currentTimeMillis()

  def duration(startTime: Long): Long = {
    System.currentTimeMillis() - startTime
  }

  def run(): Unit = {
    import spark.implicits._
    import spark.sql
    // 注册udf
    if (p.udfJars.nonEmpty) {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
      p.udfJars.foreach(f => {
        sql(s"add jar $f")
      })
    }
    if (p.udfs.nonEmpty) {
      p.udfs.foreach(f => {
        sql(s"create temporary function ${f._1} as '${f._2}'")
      })
    }


    val df = spark.sql(p.sql)

    val config = KafkaTools.getRichConfig(p.bootstrapServer, p.kafkaProducerConf)
    logger.info(s"kafka配置: $config")
    val producerBC = KafkaTools.getKafkaProducerPoolBroadcast(spark, config)
    val totalAccum = spark.sparkContext.longAccumulator("total")
    val succeedAccum: LongAccumulator = spark.sparkContext.longAccumulator("succeed")
    val failedAccum = spark.sparkContext.longAccumulator("failed")

    val startTime = System.currentTimeMillis()
    df.repartition(p.numPartitions).rdd.foreachPartition(iter => {
      val producer = producerBC.value
      iter.foreach(r => {
        val record = r.getAs[String](0)
        totalAccum.add(1L)
        val mark = producer.send(p.topic, record)
        if (mark) {
          succeedAccum.add(1L)
        } else {
          failedAccum.add(1L)
        }
      })
    })

    logger.info(s"原始数据数目:${df.count()}")
    logger.info(s"导入kafka数据总量:${totalAccum.value}")
    logger.info(s"导入kafka成功数据量:${succeedAccum.value}")
    logger.info(s"导入kafka失败数据量:${failedAccum.value}")
    logger.info(s"导入kafka时长:${duration(startTime)}")
  }
}
