package com.mob.dataengine.engine.core.portrait.crowd

import com.mob.dataengine.engine.core.jobsparam.PortraitCalculationParam
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

/**
 * @author juntao zhang
 */
package object calculation {

  case class JobStatus(needSuccess: Int) {
    var successNums: Int = 0
    var failureNums: Int = 0

    def addSuccess(status: Int): Unit = this.successNums += 1

    def addFailure(status: Int): Unit = this.failureNums += 1

    def finished: Boolean = this.needSuccess == this.successNums + this.failureNums
  }

  case class JobContext(
    job: com.mob.dataengine.engine.core.jobsparam.JobContext[PortraitCalculationParam],
    sparkOpt: Option[SparkSession],
    labelHiveTables: LabelHiveTables,
    jobStatus: JobStatus,
    useLocal: Boolean
  ) extends Serializable {
    lazy val spark: SparkSession =
      if (sparkOpt.isEmpty) {
        sparkOpt.getOrElse(setMaster(
          SparkSession.builder().appName(job.jobCommon.jobId).enableHiveSupport()).getOrCreate())
      } else {
        sparkOpt.get
      }


    def setMaster(builder: SparkSession.Builder): SparkSession.Builder = {
      if (useLocal) builder.master("local")
      builder
    }

    def release(): Unit = {
//      if (sparkOpt.isEmpty) spark.stop()
      this.spark.sql(
        """
          |drop temporary function explode_tags
        """.stripMargin)
    }

    def prepare(): JobContext = {
      /* 注册临时函数 */
      this.spark.sql(
        """create temporary function explode_tags
          |as 'com.youzu.mob.java.udtf.ExplodeTags'""".stripMargin)
      /* 动态分区 */
      this.spark.sql("set hive.exec.dynamic.partition=true")
      this.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

      this
    }

    lazy val uuids: Set[String] = job.params.map(_.output.uuid).toSet
  }

  object JobContext {
    @transient private val logger = Logger.getLogger(this.getClass)

    def apply(sparkOpt: Option[SparkSession],
      job: com.mob.dataengine.engine.core.jobsparam.JobContext[PortraitCalculationParam]
      , js: JobStatus, useLocal: Boolean = false): JobContext = {
      //      val lhts = LabelHiveTables(job)
      val lhts = LabelHiveTables(job)
      logger.info(lhts)
      new JobContext(job, sparkOpt, lhts, js, useLocal).prepare()
    }
  }

  case class SourceData(sourceDF: DataFrame, deviceBF: Broadcast[BloomFilter])

}
