package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.{ExitCode, JobCommon}
import com.mob.dataengine.commons.enums.DeviceType
import com.mob.dataengine.commons.pojo.{ParamInput, ParamOutput, ParamOutputV2}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, JobContext, ProfileScoreParam}
import com.mob.dataengine.rpc.RpcClient
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: zhangnw
 * Date: 2018-07-31
 * Time: 12:07
 *
 */
object ProfileCalScore extends BaseJob[ProfileScoreParam]{

  case class Param(paramId: String, input: ParamInput, output: ParamOutputV2)

  case class ProfileCalTaskJob(
    spark: SparkSession,
    p: ProfileScoreParam,
    jobCommon: JobCommon
  ) {
    def getUuidConditionStr: String = {
      p.inputs.map(input => s"'${input.uuid}'").mkString(",")
    }

    def getInputDF(): DataFrame = {
      spark.sql(
        s"""
           |SELECT
           | split(data, '\u0001')[0] device,
           | created_day AS rank_date,
           | uuid AS userid
           |FROM ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
           |WHERE uuid in ($getUuidConditionStr)
           |UNION ALL
           |SELECT
           | device,
           | day AS rank_date,
           | uuid AS userid
           |FROM ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
           |LATERAL VIEW EXPLODE(SPLIT(match_ids[${DeviceType.DEVICE.id}], ',')) t as device
           |WHERE uuid in ($getUuidConditionStr)
           |AND match_ids[${DeviceType.DEVICE.id}] is not null
      """.stripMargin
      ).limit(20000000)  // todo remove limit
    }

    def userIdUUIDMap: Map[String, String] = {
      p.inputs.map(x => "'" + x.uuid + "'") zip p.inputs.map(x => "'" + p.output.uuid + "'") toMap
    }

    def getUuidSeq: Seq[String] = {
      p.inputs.map(input => input.uuid).toSet.toSeq
    }
  }

  def run(): Unit = {
    jobContext.params.foreach(param => {
      val profileScoreJob = ProfileCalTaskJob(jobContext.spark, param, jobContext.jobCommon)
      ProfileScore.calO2OScore(jobContext.spark, profileScoreJob)
    })
  }
}
