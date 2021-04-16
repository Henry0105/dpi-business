package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.{BaseParam, Encrypt, JobInput, JobOutput}

class Apppkg2VecInput(override val inputType: String = "", val appName: String, val value: String = "")
  extends JobInput(inputType) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, appName=$appName, value=$value
     """.stripMargin
}

class Apppkg2VecOutput(override val uuid: String, val limit: Int = 30
  )extends JobOutput(uuid) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}
     """.stripMargin
}

class Apppkg2VecParam(override val inputs: Seq[Apppkg2VecInput], override val output: Apppkg2VecOutput)
  extends BaseParam(inputs, output) with Serializable {

}
