package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{JobInput, JobOutput}

class PortraitCalculationInput(override val uuid: String, val tagList: Seq[String])
  extends JobInput(uuid, 4) with Serializable {
  override def toString: String = {
    s"""
       |${super.toString}, tagList=${tagList.mkString(",")}
     """.stripMargin
  }
}

class PortraitCalculationParam(val inputs: Seq[PortraitCalculationInput], val output: JobOutput) extends Serializable
