package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.enums.DeviceType
import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.{BaseParam, Encrypt, JobInput, JobOutput}

class IdMappingInput(override val uuid: String, override val idType: Int, override val encrypt: Encrypt)
  extends JobInput(uuid = uuid, idType = idType, encrypt = encrypt) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, encrypt=$encrypt
     """.stripMargin
}

class IdMappingOutput(override val uuid: String, override val hdfsOutput: String,
  val idTypes: Seq[Int], val encrypt: Encrypt)
  extends JobOutput(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, idTypes=${idTypes.mkString(",")}, encrypt=$encrypt
     """.stripMargin
}

class IdMappingParam(override val inputs: Seq[IdMappingInput], override val output: IdMappingOutput)
  extends BaseParam(inputs, output) with Serializable {
  def idTypeEnum: DeviceType = inputs.head.idTypeEnum

  def joinField: String = s"$idTypeEnum"

  def getDeviceIdMappingHiveFields: String = {
    if (output.idTypes.isEmpty) {
      throw new IllegalArgumentException("device mapping type is empty")
    }
    (output.idTypes.map(DeviceType(_)) :+ idTypeEnum)
      .toSet[DeviceType].map(_.toString).mkString(",")
  }
}
