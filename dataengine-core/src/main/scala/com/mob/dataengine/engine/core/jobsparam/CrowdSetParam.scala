package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{JobInput, JobOutput}

class CrowdInput (override val uuid: String, override val inputType: String, val name: Option[String])
  extends JobInput(uuid, idType = 4) with Serializable {
  override def toString: String = {
    s"""
       |uuid: $uuid
       |inputType: $inputType
       |name: ${name.get}
     """.stripMargin
  }
}

class CrowdOutput (override val uuid: String, val opts: String)
  extends JobOutput(uuid, hdfsOutput = "") with Serializable {

  override def toString: String = {
    s"""
       |uuid: $uuid
       |opts: $opts
   """.stripMargin
  }
}

class CrowdSetParam (val inputs: Seq[CrowdInput], val output: CrowdOutput) {

}

