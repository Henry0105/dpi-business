package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.engine.core.jobsparam.{BaseJob, JobContext, ProfileScoreParam}
import com.mob.dataengine.engine.core.profilecal.ProfileCalScore.ProfileCalTaskJob
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: zhangnw
 * Date: 2018-07-31
 * Time: 12:07
 */
object ProfileCalHomeWorKFrequency extends BaseJob[ProfileScoreParam] {
  private[this] val logger = LoggerFactory.getLogger(ProfileHomeWorkFrequency.getClass)


  def run(): Unit = {
    jobContext.params.foreach(param => {

      val profileScoreJob = ProfileCalTaskJob(jobContext.spark, param, jobContext.jobCommon)
      ProfileHomeWorkFrequency.calO2OHomeWorkFrequency(jobContext.spark, profileScoreJob)
    })
  }
}
