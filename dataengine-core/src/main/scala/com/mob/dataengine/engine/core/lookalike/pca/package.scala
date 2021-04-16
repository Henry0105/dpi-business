package com.mob.dataengine.engine.core.lookalike

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

package object pca {

  case class JobContext(
                         job: Job,
                         sparkOpt: Option[SparkSession],
                         useLocal: Boolean
                       ) extends Serializable {
    lazy val spark: SparkSession =
      sparkOpt.getOrElse(setMaster(
        SparkSession.builder().appName(job.jobId).enableHiveSupport()).getOrCreate())

    def setMaster(builder: SparkSession.Builder): SparkSession.Builder = {
      if (useLocal) builder.master("local")
      builder
    }

    def release(): Unit = {
      if (sparkOpt.isEmpty) spark.stop()
    }

    def prepare(): JobContext = {
      /* 注册临时函数 */
      this.spark.sql(
        """create temporary function explode_tags
          |as 'com.youzu.mob.java.udtf.ExplodeTags'""".stripMargin)
      /* 动态分区 */
      /** this.spark.sql("set hive.exec.dynamic.partition=true")
       * this.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
       */

      this
    }
  }

  object JobContext {
    @transient private val logger = Logger.getLogger(this.getClass)
    def apply(sparkOpt: Option[SparkSession], json: String, useLocal: Boolean = false): JobContext = {
      val job = Job(json)
      logger.info(job)
      new JobContext(job, sparkOpt, useLocal).prepare()
    }
  }

  case class Job(
                  jobId: String,
                  jobName: String,
                  param: Param
                ) extends Serializable {
    override def toString: String =
      s"job(jobId:$jobId,\njobName:$jobName,\nparam:$param)"

  }

  object Job {
    implicit val _ = DefaultFormats

    def apply(job: String): Job = {
      parse(job).transformField {
        case ("job_id", x) => ("jobId", x)
        case ("job_name", x) => ("jobName", x)
      }.extract[Job]
    }
  }

  case class Param(calType: Type) {
    override def toString: String = s"\n$calType\n"
  }

  case class Type(name: Option[String], value: Int) {
    override def toString: String = s"Type(name=$name,value=$value)"
  }
}
