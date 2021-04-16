package com.mob.dataengine.engine.core.profile.export.bt

import com.mob.dataengine.commons.enums.JobName
import com.mob.dataengine.commons.exceptions.DataengineException
import com.mob.dataengine.commons.pojo.{MatchInfo, RpcResponse}
import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}
import com.mob.dataengine.commons.utils.AppUtils
import com.mob.dataengine.commons.{DeviceSrcReader, ExitCode}
import com.mob.dataengine.engine.core.jobsparam.BaseJob
import com.mob.dataengine.engine.core.jobsparam.profile.export.bt.ProfileBatchBackTrackerParam
import com.mob.dataengine.rpc.RpcClient
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

/**
 * @author juntao zhang
 */
object Bootstrap extends BaseJob[ProfileBatchBackTrackerParam] {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  override def run(): Unit = {
    val spark = jobContext.spark
    import spark.implicits._
    val hubService: DataHubService = DataHubServiceImpl(spark)

    try {
      // todo 需要容错
      jobContext.params.foreach(p => {
        val inputDF = p.inputs.map(input =>
          DeviceSrcReader.toDFV2(jobContext.spark, input, input.trackDay, input.trackDayIndex, true, hubService))
          .reduce(_ union _).cache().select(col("id").as("device"), col("day"), col("data"))

        val profileIds = p.inputs.flatMap(_.profileIds).distinct

        val tracker = ProfileBatchBackTracker(
          spark,
          inputDF,
          minDay = p.inputs.head.minDay,
          profileIds,
          p.inputs.head.trackDay,
          p.output.uuid,
          p.output.outputHDFSLanguage,
          p.output.limit,
          Option(p.output.hdfsOutput)
        )
        tracker.submit()
        if (jobContext.jobCommon.hasRPC()) {
          val matchInfo = MatchInfo(jobContext.jobCommon.jobId, p.output.uuid, inputDF.count,
            tracker.matchCnt, tracker.outCnt)
          val resp = RpcResponse(
            JobName.PROFILE_EXPORT_BACK_TRACK, 2, "", spark.sparkContext.applicationId
          ).put("match_info", s"""2\u0001${p.output.uuid}\u0002${matchInfo.toJsonString()}""")
          RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort, RpcResponse.mkString(resp))
        }
      })
    } catch {
      case e@DataengineException(msg, code) =>
        if (jobContext.jobCommon.hasRPC()) {
          val resp = RpcResponse(JobName.PROFILE_EXPORT_BACK_TRACK, code, msg, spark.sparkContext.applicationId)
          RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort, RpcResponse.mkString(resp))
        }
        logger.error(s"$msg\t$code", e)
      case e: Throwable => throw e
    }
  }
}
