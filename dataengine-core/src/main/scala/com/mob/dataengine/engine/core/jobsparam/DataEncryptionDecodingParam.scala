package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.{BaseParam, Encrypt, JobInput, JobOutput}

class DataEncryptionDecodingInput(override val uuid: String, override val idType: Int)
  extends JobInput(uuid, idType) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, uuid=$uuid, idType=$idType
     """.stripMargin
}

class DataEncryptionDecodingOutput(override val uuid: String, override val hdfsOutput: String,
  val dataProcess: Int, val limit: Long)
  extends JobOutput(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, hdfsOutput=$hdfsOutput, dataProcess=$dataProcess, limit=$limit
     """.stripMargin
}

class DataEncryptionDecodingParam(override val inputs: Seq[DataEncryptionDecodingInput],
  override val output: DataEncryptionDecodingOutput)
  extends BaseParam(inputs, output) with Serializable {
  def dataProcess: Int = output.dataProcess
  def idTypeEnum: DeviceType = inputs.head.idTypeEnum

  def field: String = s"$idTypeEnum"
}
