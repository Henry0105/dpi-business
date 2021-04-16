package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, CrowdSetParam, JobContext}
import com.mob.dataengine.commons.pojo._
import com.mob.dataengine.commons.{DeviceCacheWriter, DeviceSrcReader}
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.parsing.combinator.JavaTokenParsers

/**
 * 人群交并差
 */
object CrowdSet extends BaseJob[CrowdSetParam] with JavaTokenParsers  {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  override def run(): Unit = {
    val spark = jobContext.spark
    import spark.implicits._
    jobContext.params.foreach{
      p => {
        val map = new mutable.HashMap[String, RDD[String]]()
        p.inputs.foreach(input => {
          val rdd = DeviceSrcReader.toRDD2(jobContext.spark, input)
          map += (input.name.get -> rdd.cache())
        })
        val ret = CrowdSetOptsParser(p.output.opts, map).run()
        if (ret.isDefined) {
          var dataset = ret.get.toDF("data")
          dataset.persist(StorageLevel.MEMORY_AND_DISK)
          val total = dataset.count()
          val par = {
            val t = (total / 3000000).toInt
            if (t > 0) t else 1
          }
          dataset = dataset.coalesce(par)
          DeviceCacheWriter.insertTable2(jobContext.spark, jobContext.jobCommon, p.output, dataset,
            biz = s"${JobName.getId(jobContext.jobCommon.jobName)}|${DeviceType.DEVICE.id}")
          dataset.unpersist()
          // todo 发mq统一
          if (jobContext.jobCommon.hasRPC()) {
            val outCnt = new OutCnt
            outCnt.set("device_cnt", total)
            val matchInfo = MatchInfo(jobContext.jobCommon.jobId, p.output.uuid, 0, total, outCnt)
            RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort,
              s"2\u0001${p.output.uuid}\u0002${matchInfo.toJsonString()}")
          }
        } else {
          println("parse error")
          System.exit(2)
        }
        map.foreach(rdd => rdd._2.unpersist())
        map.clear()
      }
    }
  }

  case class CrowdSetJob(
    spark: SparkSession,
    inputDF: DataFrame,
    p: CrowdSetParam,
    jobCommon: JobCommon
  )
}
