package com.mob.dataengine.engine.core.mapping.dataprocessor

import com.mob.dataengine.commons.enums.JobName
import com.mob.dataengine.commons.pojo.RpcResponse
import com.mob.dataengine.commons.utils.DateUtils
import com.mob.dataengine.commons.DeviceSrcReader
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, DataEncryptionDecodingParam, JobContext}
import com.mob.dataengine.rpc.RpcClient
import org.slf4j.LoggerFactory

object DataEncryptionDecodingLaunch extends BaseJob[DataEncryptionDecodingParam] {

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  var rpc_code: Int = 0
  var rpc_msg: String = _

  override def run(): Unit = {
    val spark = jobContext.spark
    import spark.implicits._

    // TODO 根据job.param进行处理
    jobContext.params.foreach{ p =>
      println(p)
      val idRDD = p.inputs.map(input => DeviceSrcReader.toRDD2(jobContext.spark, input)).reduce(_ union _)
      idRDD.cache()
      val smallDeviceDF = idRDD.toDF(p.field)
      val dataProcess = p.dataProcess

      // 1.脱敏 2.逆向
      if (dataProcess == 1) {
        val dataEncryption = DataEncryption(
          spark = spark,
          fieldName = p.field,
          idType = p.inputs.head.idType,
          outputDay = if (jobContext.jobCommon.day.isEmpty) DateUtils.currentDay() else jobContext.jobCommon.day,
          outputUuid = p.output.uuid,
          limit = p.output.limit,
          jobName = jobContext.jobCommon.jobName,
          hdfsOutputPath = p.output.hdfsOutput,
          sourceData = smallDeviceDF
        )
        // 数据脱敏
        dataEncryption.submit()
        rpc_msg = "DATA_ENCRYPTION_SUCCESS"
      } else if (dataProcess == 2) {
        val dataDecoding = DataDecoding(
          spark = spark,
          fieldName = p.field,
          idType = p.inputs.head.idType,
          outputDay = if (jobContext.jobCommon.day.isEmpty) DateUtils.currentDay() else jobContext.jobCommon.day,
          outputUuid = p.output.uuid,
          limit = p.output.limit,
          hdfsOutputPath = p.output.hdfsOutput,
          jobName = jobContext.jobCommon.jobName,
          sourceData = smallDeviceDF
        )
        // 数据逆向
        dataDecoding.submit()
        rpc_msg = "DATA_DECODING_SUCCESS"
      }
// todo
//      if (jobContext.jobCommon.hasRPC()) {
//        val resp = RpcResponse(JobName.DATA_ENCRYPT_DECODE, rpc_code, rpc_msg, spark.sparkContext.applicationId)
//        RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort, RpcResponse.mkString(resp))
//      }
    }
  }
}
