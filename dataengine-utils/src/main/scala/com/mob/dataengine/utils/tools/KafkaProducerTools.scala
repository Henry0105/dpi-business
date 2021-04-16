package com.mob.dataengine.utils.tools

import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.RetriableException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object KafkaTools {
  @volatile private var kafkaProducerPool: Broadcast[KafkaProducerPool[String, String]] = _

  /**
   * @see [[ProducerConfig]] config和对应的文档
   *      这是官网的生产者config介绍链接:
   *      <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>
   * @param bootstrap    指定请求的kafka集群列表
   * @param acks         ACK应答级别
   * @param retries      重试次数
   * @param batchSize    批次大小
   * @param lingerMs     等待大小
   * @param bufferMemory RecordAccumulator缓冲区大小
   * @return 生产者的配置
   */
  def getSimpleConfig(bootstrap: String, acks: String = "0", retries: Int = 3, batchSize: Int = 16384,
                      lingerMs: Int = 0, bufferMemory: Int = 33554432): Map[String, Object] = {
    Map(
      // 1 指定请求的kafka集群列表
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
      // 2 ACK应答级别
      ProducerConfig.ACKS_CONFIG -> acks,
      // 3 重试次数
      ProducerConfig.RETRIES_CONFIG -> retries.toString,
      // 4 批次大小
      ProducerConfig.BATCH_SIZE_CONFIG -> batchSize.toString,
      // 5 等待大小
      ProducerConfig.LINGER_MS_CONFIG -> lingerMs.toString,
      // 6 RecordAccumulator缓冲区大小
      ProducerConfig.BUFFER_MEMORY_CONFIG -> bufferMemory.toString,
      // 7 key,Value的序列化
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
    )
  }

  /**
   * 可以配置更丰富的kafka参数,以kv形式传进来
   */
  def getRichConfig(bootstrap: String, configs: Map[String, Object]): Map[String, Object] = {
    val sConfig = getSimpleConfig(bootstrap)
    sConfig ++ configs
  }

  def getRichConfig(bootstrap: String, configs: Seq[(String, Object)]): Map[String, Object] = {
    getRichConfig(bootstrap, configs.toMap)
  }

  def getKafkaProducerPoolBroadcast(spark: SparkSession, config: Map[String, Object])
  : Broadcast[KafkaProducerPool[String, String]] = {
    if (kafkaProducerPool == null) {
      synchronized {
        if (kafkaProducerPool == null) {
          kafkaProducerPool = spark.sparkContext.broadcast(this.getKafkaProducerPool[String, String](config))
        }
      }
    }
    kafkaProducerPool
  }


  def getKafkaProducerPool[K, V](config: Properties): KafkaProducerPool[K, V] = {
    KafkaProducerPool[K, V](config)
  }

  def getKafkaProducerPool[K, V](config: Map[String, Object]): KafkaProducerPool[K, V] = {
    KafkaProducerPool[K, V](config)
  }
}

class KafkaProducerPool[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  // 后续在spark主程序中使用时，将kafka pool广播出去到每个executor里面了,
  // 然后到每个executor中，当用到的时候，会实例化一个producer，这样就不会有NotSerializableExceptions的问题了。
  lazy val producer: KafkaProducer[K, V] = createProducer()

  /**
   * 发送数据到kafka
   *
   * @param topic kafka的主题
   * @param value message 的 value
   */
  def send(topic: String, value: V): Boolean = {
    var mark = true
    val record = new ProducerRecord[K, V](topic, value)
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          // 消息发送成功, 处理recordMetadata, 如分区, 位移等
          // do nothing...
        } else {
          mark = false
          if (exception.isInstanceOf[RetriableException]) {
            // 处理可重试瞬时异常, 如LeaderNotAvailableException|NotControllerException|NetworkException
            println(s"record[$value] sent failed to topic[$topic] cause of RetriableException")
          } else {
            // 处理不可重试异常
            println(s"record[$value] sent failed to topic[$topic] cause of Other Exception")
          }
        }
      }
    })
    mark
  }


}

object KafkaProducerPool {

  import scala.collection.JavaConverters._

  def apply[K, V](config: Map[String, Object]): KafkaProducerPool[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config.asJava)
      sys.addShutdownHook {
        // 当发送executor中的jvm shutdown时，kafka能够将缓冲区的消息发送出去。
        producer.close()
      }
      producer
    }
    new KafkaProducerPool(createProducerFunc)
  }

  def apply[K, V](config: Properties): KafkaProducerPool[K, V] = {
    apply(config)
  }
}