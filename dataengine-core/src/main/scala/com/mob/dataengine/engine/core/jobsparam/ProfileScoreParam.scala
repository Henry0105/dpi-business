package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{BaseParam, JobInput, JobOutput}

class ProfileScoreInput(override val uuid: String)
  extends JobInput(uuid, idType = 4) {

}
class ProfileScoreOutput(override val uuid: String, override val hdfsOutput: String = "",
  val threshold: Option[Map[String, Double]], val order: Option[Seq[Map[String, Any]]],
  val limit: Option[Int])
extends JobOutput(uuid, hdfsOutput = "") {

}

class ProfileScoreParam (override val inputs: Seq[ProfileScoreInput], override val output: ProfileScoreOutput)
  extends BaseParam(inputs, output) {

}
