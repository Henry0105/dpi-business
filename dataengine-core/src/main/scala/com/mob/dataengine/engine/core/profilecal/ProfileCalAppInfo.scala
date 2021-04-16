package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.engine.core.jobsparam.{BaseJob, JobContext, ProfileScoreParam}
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: zhangnw
 * Date: 2018-07-31
 * Time: 12:07
 */
object ProfileCalAppInfo extends BaseJob[ProfileScoreParam] {

  override def run(): Unit = {
    jobContext.params.foreach(param => {
      val profileScoreJob = ProfileCalTaskJob(jobContext.spark, param, jobContext.jobCommon)
      ProfileAppInfo.calO2OAppInfo(jobContext.spark, profileScoreJob)
    })
  }
}
