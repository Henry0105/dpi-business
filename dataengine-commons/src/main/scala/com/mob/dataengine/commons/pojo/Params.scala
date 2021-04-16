package com.mob.dataengine.commons.pojo

import com.mob.dataengine.commons.enums.DeviceType
import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.utils.{DateUtils, Md5Helper}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

/**
 * @author juntao zhang
 */
trait ParamInterface

case class ParamInput(
  var uuid: Option[String], // todo must be set
  name: Option[String],
  value: String,
  inputType: Option[String] = None,
  compressType: Option[String] = None,
  idType: Option[Int] = None,
  encryptType: Option[Int] = None,
  aesInfo: Option[AesInfo] = None,
  sep: Option[String] = None
) {
  if (!uuid.isDefined) {
    if (isHiveCache) {
      uuid = Some(value)
    } else if (isDfsFile) {
      // TODO 未来uuid必须要传
      uuid = Some(Md5Helper.encryptMd5_32(value))
    }
  }

  def getUUID: String = uuid.get

  override def toString: String = s"Input(uuid=$uuid,name=$name,value=$value," +
    s"inputType=$inputType,compressType=$compressType,idType=$idType," +
    s"encryptType=$encryptType,aesInfo=$aesInfo,sep=$sep)"

  def isDfsFile: Boolean = {
    inputType.isDefined && inputType.get == "dfs"
  }

  def isHiveCache: Boolean = {
    inputType.isDefined && inputType.get == "uuid"
  }

  def idTypeEnum: Option[DeviceType] = idType.map(DeviceType(_))
}

object ParamInput {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def apply(json: String): ParamInput = {
    JsonMethods.parse(json).transformField {
      case ("compress_type", x) => ("compressType", x)
      case ("input_type", x) => ("inputType", x)
    }.extract[ParamInput]
  }
}

case class ParamOutput(
  var uuid: Option[String],
  value: String,
  outputType: Option[String],
  var day: Option[String] = Some(DateUtils.currentDay()),
  limit: Option[Int] = Some(5000000)
) {

  def isDfsFile: Boolean = {
    outputType.isDefined && outputType.get == "dfs"
  }

  def isHiveCache: Boolean = {
    outputType.isDefined && outputType.get == "uuid"
  }

  if (!uuid.isDefined) {
    if (isHiveCache) {
      uuid = Some(value)
    } else {
      // TODO 未来uuid必须要传
      uuid = Some(Md5Helper.encryptMd5_32(value))
    }
  }

  if (!day.isDefined) {
    day = Some(DateUtils.currentDay())
  }

  def getUUID: String = uuid.get

  override def toString: String = s"Output(uuid=$uuid,value=$value,outputType=$outputType,day=${day.get})"

}

object ParamOutput {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def apply(json: String): ParamOutput = {
    JsonMethods.parse(json).transformField {
      case ("output_type", x) => ("outputType", x)
    }.extract[ParamOutput]
  }
}

case class AesInfo(key: String, iv: String)

case class ParamOutputV2(
  uuid: String,
  value: String,
  limit: Option[Int],
  day: String
) {


  //def this(uuid: String, value: String) = this(uuid, value, Some(Int.MaxValue), DateUtils.currentDay())

  // def toParamOutput: ParamOutput = new ParamOutput(uuid = Some(uuid), value = value, outputType = Some("uuid"))
}

object ParamOutputV2 {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def apply(json: String): ParamOutput = {
    JsonMethods.parse(json).extract[ParamOutput]
  }
}
