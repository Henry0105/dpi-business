package com.mob.dataengine.engine.core.profilecal

import java.util.{Calendar, Date}

import com.mob.dataengine.commons.ExitCode
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, JobContext, ProfileScoreParam}
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob
import com.mob.dataengine.rpc.RpcClient
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: zhangnw
 * Date: 2018-07-31
 * Time: 12:07
 */
object ProfileCalSourceAndFlow extends BaseJob[ProfileScoreParam] {

  override def run(): Unit = {
    jobContext.params.foreach(param => {
      val profileScoreJob = ProfileCalTaskJob(jobContext.spark, param, jobContext.jobCommon)

      val fdf = FastDateFormat.getInstance("yyyyMMdd")
      val today: String = fdf.format(new Date())
      val day = org.apache.commons.lang3.time.DateUtils.parseDate(today, "yyyyMMdd")
      val calendar = Calendar.getInstance()
      calendar.setTime(day)
      calendar.add(Calendar.DAY_OF_MONTH, -10)
      val preDay = fdf.format(calendar.getTime)
      calendar.setTime(day)
      calendar.add(Calendar.DAY_OF_MONTH, 10)
      val afterDay = fdf.format(calendar.getTime)

      ProfileSourceFlow.calO2OSourceFlow(jobContext.spark, profileScoreJob, today, preDay, afterDay)
    })
  }
}
