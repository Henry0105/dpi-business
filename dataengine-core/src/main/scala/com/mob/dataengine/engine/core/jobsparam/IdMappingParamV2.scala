package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.enums.DeviceType
import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.{BaseParam, Encrypt, JobInput, JobOutput}

class IdMappingV2Input(override val uuid: String, override val idType: Int, override val encrypt: Encrypt,
                       override val sep: Option[String])
  extends JobInput(uuid = uuid, idType = idType, encrypt = encrypt) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, encrypt=$encrypt
     """.stripMargin
}

class IdMappingV2Output(override val uuid: String, override val hdfsOutput: String,
  val idTypes: Seq[Int], val encrypt: Encrypt, val limit: Option[Int], val matchLimit: Option[Int] = None)
  extends JobOutput(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, idTypes=${idTypes.mkString(",")}, encrypt=$encrypt, limit=$limit,
       |matchLimit=$matchLimit
     """.stripMargin

  def idTypesEnum: Seq[DeviceType] = idTypes.map(DeviceType(_))
}

class IdMappingV2Param(override val inputs: Seq[IdMappingV2Input], override val output: IdMappingV2Output)
  extends BaseParam(inputs, output) with Serializable {
  val idTypeEnum: DeviceType = inputs.head.idTypeEnum

  val origJoinField: String = s"$idTypeEnum"
  val joinSuffix: String = inputs.head.encrypt.suffix
  val joinField: String = s"$origJoinField$joinSuffix"

  val getOutputFieldsWithKey: Seq[String] = {
    val limit = if (output.matchLimit.nonEmpty) output.matchLimit.get else 1
    val fieldLimitTuple = output.idTypesEnum.filterNot(key => key.equals(idTypeEnum)).map(field => (field, limit))
    fieldLimitTuple.map(f => s"latest(${f._1}, ${f._1}_tm, ${f._2}) as ${f._1}") :+ idTypeEnum.toString
  }

  val sep: Option[String] = inputs.head.sep
}
