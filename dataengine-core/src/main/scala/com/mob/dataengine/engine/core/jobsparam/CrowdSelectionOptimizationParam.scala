package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{BaseParam, JobInput, JobOutput}

class CrowdSelectionOptimizationInput(override val uuid: String, override val idType: Int,
  val option: Map[String, String])
  extends JobInput(uuid, idType) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, option=$option
     """.stripMargin
}

class CrowdSelectionOptimizationOutput(override val uuid: String, override val hdfsOutput: String, val limit: Int)
  extends JobOutput(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, limit=$limit
     """.stripMargin
}

class CrowdSelectionOptimizationParam(override val inputs: Seq[CrowdSelectionOptimizationInput],
  override val output: CrowdSelectionOptimizationOutput)
  extends BaseParam(inputs, output) with Serializable {

}
