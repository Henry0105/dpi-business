package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{BaseParam, JobInput, JobOutput}

class CrowdPortraitEstimationInput(override val uuid: String,
  val tagList: Array[String])
  extends JobInput(uuid, 4) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, tagList=${tagList.mkString(",")}
     """.stripMargin
}

class CrowdPortraitEstimationOutput(override val uuid: String, override val hdfsOutput: String)
  extends JobOutput(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}
     """.stripMargin
}

class CrowdPortraitEstimationParam(override val inputs: Seq[CrowdPortraitEstimationInput],
  override val output: CrowdPortraitEstimationOutput)
  extends BaseParam(inputs, output) with Serializable {

}
