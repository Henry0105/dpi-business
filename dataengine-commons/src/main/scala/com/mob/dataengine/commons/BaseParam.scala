package com.mob.dataengine.commons

import com.mob.dataengine.commons.annotation.code.explanation
import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.enums.SecDeviceType.SecDeviceType
import com.mob.dataengine.commons.enums.InputType.InputType
import com.mob.dataengine.commons.enums.{DeviceType, SecDeviceType, InputType}
import com.mob.dataengine.commons.utils.{AesHelper, Md5Helper, Sha256Helper}
import com.youzu.mob.java.udf.ReturnImei15
import org.apache.commons.lang3.StringUtils
import com.mob.udf.PidEncrypt
import com.mob.udf.AesEncrypt

case class Encrypt(encryptType: Int, args: Option[Map[String, String]] = None) {
  def compute(content: String): String = {
    if (StringUtils.isBlank(content)) {
      null
    } else {
      encryptType match {
        case 0 => content
        case 1 => Md5Helper.entryMD5_32(content)
        case 2 => AesHelper.encodeAes(args.get("key"), args.get("iv"), content)
        case 3 => Md5Helper.entryMD5_32(content).substring(8, 24)
        case 4 => Sha256Helper.entrySHA256(content)
        case 5 => new PidEncrypt().evaluate(content)   // pid(md5)
        case 6 => new AesEncrypt().evaluate(Sha256Helper.entrySHA256(content))  // pid(sha256)
        case 7 => new AesEncrypt().evaluate(content)  // 合规版aes加密
      }
    }
  }

  // 解密 md5 与 sha256 无法解密 因此只支持AES解密
  def decode(content: String): String = {
    if (StringUtils.isBlank(content)) {
      null
    } else {
      encryptType match {
        case 2 => AesHelper.decodeAes(args.get("key"), args.get("iv"), content)
        case _ => content
      }
    }
  }

  def suffix: String = {
    encryptType match {
      case 0 => ""
      case 1 => "_md5"
      case _ => "_aes"
    }
  }

  def isNone: Boolean = encryptType == 0
  def isMd5: Boolean = encryptType == 1
  def isAes: Boolean = encryptType == 2
  def isMidMd5: Boolean = encryptType == 3
  def isSha256: Boolean = encryptType == 4
  def isPid: Boolean = encryptType == 5
  def isPidSha256: Boolean = encryptType == 6
  def isSecAes: Boolean = encryptType == 7
}

case class Transform(transformMap: Option[Map[String, Seq[String]]]) {

  trait TransformDetail extends Serializable {
    def execute(content: String): String
  }

  /**
   * 进行大小写转换
   */
  private case class Caps(flag: Seq[String]) extends TransformDetail {
    override def execute(content: String): String = {
      flag.head match {
        case "0" => content.toUpperCase
        case _ => content.toLowerCase
      }
    }
  }

  /**
   * 从输入内容中删除对应的字符
   */
  private case class Delete(symbol: Seq[String]) extends TransformDetail {
    override def execute(content: String): String = {
      symbol.foldLeft(content)(_.replaceAll(_, ""))
    }
  }

  /**
   * imei补位
   */
  private case class FillImei(flag: Seq[String]) extends TransformDetail {
    override def execute(content: String): String = {
      if (!ReturnImei15.isNumeric(content) || content.length != 14) {
        content
      } else {
        ReturnImei15.evaluate(content)
      }
    }
  }

  private case class FillIeid(flag: Seq[String]) extends TransformDetail {
    override def execute(content: String): String = {
      if (!ReturnImei15.isNumeric(content) || content.length != 14) {
        content
      } else {
        ReturnImei15.evaluate(content)
      }
    }
  }

  /**
   * 每一对kv的处理
   */
  private def toOnce(tup: (String, Seq[String]), list: Seq[TransformDetail]): Seq[TransformDetail] = {
    val (transType, transValue) = tup
    val obj: TransformDetail =
      transType match {
        case "lower" =>
          if (transValue.nonEmpty && transValue.head != null) Caps(transValue)
          else null
        case "delete" =>
          if (transValue.isEmpty || (transValue.length == 1 && transValue.head == null)) null
          else Delete(transValue)
        case "fillImei" =>
          if (transValue.nonEmpty && transValue.head != null && transValue.head.equals("1")) FillImei(transValue)
          else null
        case "fillIeid" =>
          if (transValue.nonEmpty && transValue.head != null && transValue.head.equals("1")) FillIeid(transValue)
          else null
        case _ => null
      }

    obj match {
      case _: FillImei =>
        list :+ obj
      case _: FillIeid =>
        list :+ obj
      case _ =>
        obj +: list
    }
  }

  /**
   * 将传入来的transformMap根据kv匹配成类型为TransformDetail对象的Seq
   */
  def mapToObjSeq: Seq[TransformDetail] = {
    val seq = Seq.empty[TransformDetail]
    val tmpSeq = if (transformMap.isDefined) {
      transformMap.get.foldRight(seq)(toOnce)
    } else {
      seq
    }
    tmpSeq.filter(_ != null)
  }

  /**
   * 对字符串进行transform
   *
   * @param content 输入的字符串
   * @return 转换后的字符串
   */
  def execute(content: String): String = {
    val seq = this.mapToObjSeq
    if (seq.nonEmpty) {
      seq.foldLeft(content)((content: String, obj: TransformDetail) => obj execute content)
    } else {
      content
    }
  }
}

@explanation("参数input trait")
trait InputTrait {
  val uuid: String
  val idType: Int
  val idx: Option[Int]
  val sep: Option[String]
  val inputType: String
}

@explanation("参数output trait")
trait OutputTrait {
  val uuid: String
  val hdfsOutput: String
}

class JobInput(override val uuid: String, override val idType: Int = 4,
               override val idx: Option[Int] = None, override val sep: Option[String] = None,
               override val inputType: String = "uuid", val encrypt: Encrypt = Encrypt(0))
  extends InputTrait with Serializable {
  def idTypeEnum: DeviceType = DeviceType(idType)

  def idTypeEnumSec: SecDeviceType = SecDeviceType(idType)

  def inputTypeEnum: InputType = InputType.withName(inputType)

  override def toString: String =
    s"""
       |uuid=$uuid, idType=$idType, idx=$idx, sep=$sep, inputType=$inputType,
       |encrypt.encryptType=${encrypt.encryptType}, encrypt.args = ${encrypt.args}
     """.stripMargin
}

class JobOutput(override val uuid: String, override val hdfsOutput: String = "") extends OutputTrait with Serializable {
  override def toString: String =
    s"""
       |uuid=$uuid, hdfsOutput=$hdfsOutput
     """.stripMargin
}

class JobCommon(val jobId: String, val jobName: String, val rpcHost: String, val rpcPort: Int, val day: String)
  extends Serializable {
  def hasRPC(): Boolean = {
    StringUtils.isNotBlank(rpcHost) && rpcPort != 0
  }

  override def toString: String =
    s"""
       |jobId=$jobId, jobName=$jobName, rpcHost=$rpcHost, rpcPort=$rpcPort
     """.stripMargin
}

class BaseParam(val inputs: Seq[JobInput], val output: JobOutput) extends Serializable {
  override def toString: String =
    s"""
       |inputs=${inputs.map(_.toString).mkString(",")}, output=$output
     """.stripMargin
}

/** todo:为了解决继承BaseJob2和使用toDFV2的问题,修改继承[[InputTrait]],改为[[JobInput]];
 *       全部改造完后，修改会继承[[InputTrait]]，弃用[[JobInput]] */
class JobInput2(override val uuid: String, override val idType: Int = 4,
                override val sep: Option[String] = None, val header: Int = 0,
                override val idx: Option[Int] = None, val headers: Option[Seq[String]] = None,
                override val encrypt: Encrypt = Encrypt(0), override val inputType: String = "uuid")
  extends JobInput(uuid = uuid, idType = idType, idx = idx, sep = sep, inputType = inputType) with Serializable {

  def seedSchema: SeedSchema = SeedSchema(InputType.withName(inputType), idType,
    sep.getOrElse(","), headers.getOrElse(Seq.empty[String]), idx.getOrElse(1), uuid, encrypt)

  override def toString: String =
    s"""
       |uuid=$uuid, idType=$idType, sep=$sep, idx=$idx,
       |header=$header, headers=$headers, idx=$idx, inputType=$inputType,
       |encrypt.encryptType=${encrypt.encryptType}, encrypt.args = ${encrypt.args}
     """.stripMargin
}

class JobOutput2(override val uuid: String, override val hdfsOutput: String = "",
                 val limit: Option[Int] = Some(-1), val keepSeed: Int = 1)
  extends OutputTrait with Serializable {
  override def toString: String =
    s"""
       |uuid=$uuid, hdfsOutput=$hdfsOutput, limit=$limit, keepSeed=$keepSeed
     """.stripMargin
}

class BaseParam2(val inputs: Seq[JobInput2], val output: JobOutput2)
  extends Serializable {
  @explanation("枚举值")
  val inputIdTypeEnum: DeviceType = inputs.head.idTypeEnum
  val inputIdTypeEnumSec: SecDeviceType = inputs.head.idTypeEnumSec
  val inputIdType: Int = inputs.head.idType
  @explanation("数据分隔符")
  val sep: Option[String] = inputs.head.sep
  @explanation("做transform 数据的 index")
  val idx: Option[Int] = inputs.head.idx
  @explanation("是否包含头信息，1 为包含头信息，0 为不包含")
  val header: Int = inputs.head.header
  @explanation("每列的列名")
  var headers: Option[Seq[String]] = {  // todo 这块应该在解析sql的时候做更好
    // 当输入为sql的时候,没有指定headers,设置一个默认值
    if (header > 0 && (inputs.head.headers.isEmpty || inputs.head.headers.get.isEmpty)) {
      Some(Seq("in_id"))
    } else {
      inputs.head.headers
    }
  }
  @explanation("做transform 字段的字段名")
  val origJoinField: String = if (header > 0) headers.get(idx.getOrElse(1) - 1) else "in_id"
  @explanation("数据加密类型 0:不加密, 1:MD5_32, 2:AES, 3:MD5_16, 4:SHA256")
  val inputEncrypt: Encrypt = inputs.head.encrypt
  @explanation("匹配数据的字段前缀")
  val fieldName: String = "out_" + inputIdTypeEnum
  val fieldNameSec: String = "out_" + inputIdTypeEnumSec
  @explanation("任务的输入类型")
  val inputTypeEnum: InputType = inputs.head.inputTypeEnum

  override def toString: String =
    s"""
       |inputs=${inputs.map(_.toString).mkString(",")}, output=$output
     """.stripMargin
}