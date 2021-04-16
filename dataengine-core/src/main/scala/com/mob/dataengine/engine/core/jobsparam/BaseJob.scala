package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats

case class JobContext[T](spark: SparkSession, jobCommon: JobCommon, params: Seq[T])

abstract class BaseJob[T : Manifest] {
  implicit val formats: DefaultFormats.type = DefaultFormats
  private var jobCommon: JobCommon = _
  private var params: Seq[T] = _
  val PARAMS_KEY = "params"
  var jobContext: JobContext[T] = _

  def prepare(arg: String): Unit = {
    jobCommon = JobParamTransForm.humpConversion(arg)
      .extract[JobCommon]
    println(jobCommon)
    params = (JobParamTransForm.humpConversion(arg) \ PARAMS_KEY)
      .extract[Seq[T]]
  }

  def run(): Unit

  def main(args: Array[String]): Unit = {
    prepare(args(0))

    lazy val spark = SparkSession
      .builder()
      .appName(jobCommon.jobId)
      .enableHiveSupport()
      .getOrCreate()

    if (jobCommon.hasRPC()) {
      RpcClient.send(jobCommon.rpcHost, jobCommon.rpcPort, s"1\u0001${spark.sparkContext.applicationId}")
    }
    jobContext = JobContext(spark, jobCommon, params)
    run()
  }
}
