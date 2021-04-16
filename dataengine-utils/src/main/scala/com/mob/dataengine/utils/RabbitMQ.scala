package com.mob.dataengine.utils

import com.rabbitmq.client._

/**
 * @author juntao zhang
 */
object RabbitMQSender {
  def main(args: Array[String]): Unit = {
    val factory = new ConnectionFactory
    factory.setPort(PropUtils.RABBITMQ_PORT)
    factory.setUsername(PropUtils.RABBITMQ_USERNAME)
    factory.setPassword(PropUtils.RABBITMQ_PASSWORD)
    factory.setHost(PropUtils.RABBITMQ_HOST)
    factory.setVirtualHost(PropUtils.RABBITMQ_VIRTUAL_HOST)

    // 创建一个新的连接
    val conn = factory.newConnection
    // 获得信道
    val channel = conn.createChannel
    // 声明交换器
    val queueName = args(0)
    channel.queueDeclare(queueName, true, false, false, null)
    // 发布消息
    val messageBodyBytes = args(1).getBytes
    channel.basicPublish("", queueName, null, messageBodyBytes)

    try {
      channel.abort()
      conn.close()
    } catch {
      case e: Throwable => println(e.getMessage)
    }
  }
}

object RabbitMQReceiver {
  def main(args: Array[String]): Unit = {
    val factory = new ConnectionFactory
    factory.setPort(PropUtils.RABBITMQ_PORT)
    factory.setUsername(PropUtils.RABBITMQ_USERNAME)
    factory.setPassword(PropUtils.RABBITMQ_PASSWORD)
    factory.setHost(PropUtils.RABBITMQ_HOST)
    factory.setVirtualHost(PropUtils.RABBITMQ_VIRTUAL_HOST)
    // 建立到代理服务器到连接
    val conn = factory.newConnection
    // 获得信道
    val channel = conn.createChannel
    // 声明交换器
    val queueName = args(0)
    // 绑定队列，通过键 hola 将队列和交换器绑定起来
    channel.queueDeclare(queueName, true, false, false, null)
    // 消费消息
    channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope,
        properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        // Date timestamp = properties.getTimestamp();
        // String messageId = properties.getMessageId();
        // 确认消息
        System.out.println("消费的消息体内容：")
        System.out.println(consumerTag)
        System.out.println(envelope)
        val bodyStr = new String(body, "UTF-8")
        System.out.println(bodyStr)
        // System.out.println(String.format("时间:%s,id:%s,内容:%s",timestamp.toString(),messageId,bodyStr));
        // 如果设置autoAck=false,则不写下面一行消息就可以重复消费
        // 正常消费后加上下面一句,消息会从消息队列里删除
        // channel.basicAck(envelope.getDeliveryTag(), true);
      }
    })
  }
}
