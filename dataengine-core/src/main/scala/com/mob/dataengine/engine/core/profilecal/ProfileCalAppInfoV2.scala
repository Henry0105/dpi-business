package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.engine.core.jobsparam.{BaseJob, ProfileScoreParam}
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: zhangnw
 * Date: 2018-07-31
 * Time: 12:07
 */
object ProfileCalAppInfoV2 extends BaseJob[ProfileScoreParam] {

  override def run(): Unit = {
    jobContext.params.foreach(param => {
      val profileScoreJob = ProfileCalTaskJob(jobContext.spark, param, jobContext.jobCommon)
      ProfileAppInfoV2.calO2OAppInfo(jobContext.spark, profileScoreJob)
    })
  }
}
