package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.{DeviceCacheWriter, DeviceSrcReader}
import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.pojo.{MatchInfo, OutCnt}
import com.mob.dataengine.commons.utils.FnHelper
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, CrowdSetParamV2}
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.combinator.JavaTokenParsers

object CrowdSetV2 extends BaseJob[CrowdSetParamV2] with JavaTokenParsers {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  override def run(): Unit = {
    val spark = jobContext.spark
    import spark.implicits._

    jobContext.params.foreach {
      crowdSetParamV2 => {
        val map = new mutable.HashMap[String, RDD[String]]()
        val outCnt = new OutCnt
        crowdSetParamV2.inputs.foreach(input => {
          val rdd = DeviceSrcReader.toRDD2(jobContext.spark, input)
          map += (input.name.get -> rdd.cache())
          outCnt.set(input.name.get + "_count", rdd.count())
        })
        val ret = CrowdSetOptsParser(crowdSetParamV2.output.opts, map).run()
        if(ret.isDefined) {
          var dataSet = ret.get.toDF("data")
          dataSet.persist(StorageLevel.MEMORY_AND_DISK)
          val total = dataSet.count()
          dataSet = dataSet.coalesce(FnHelper.getCoalesceNum(total))
          DeviceCacheWriter.insertTable2(jobContext.spark, jobContext.jobCommon, crowdSetParamV2.output, dataSet,
            biz = s"${JobName.getId(jobContext.jobCommon.jobName)}|${DeviceType.DEVICE.id}")
          // 判断hdfsOutput路径
          if (!crowdSetParamV2.output.hdfsOutput.isEmpty) {
            if (crowdSetParamV2.output.limit.getOrElse(-1) != -1 ) {
              dataSet = dataSet.limit(crowdSetParamV2.output.limit.get)
            }
            dataSet.coalesce(1).write.mode(SaveMode.Overwrite)
              .option("emptyValue", null)
              .option("sep", "\t")
              .csv(crowdSetParamV2.output.hdfsOutput)
          }
          dataSet.unpersist()
          if (jobContext.jobCommon.hasRPC()) {
            val matchInfo = MatchInfo(jobContext.jobCommon.jobId, crowdSetParamV2.output.uuid, 0, total, outCnt)
            RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort,
              s"2\u0001${crowdSetParamV2.output.uuid}\u0002${matchInfo.toJsonString()}")
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
}
