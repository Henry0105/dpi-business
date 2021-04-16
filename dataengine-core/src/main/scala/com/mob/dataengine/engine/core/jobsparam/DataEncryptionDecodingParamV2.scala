package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.{BaseParam, BaseParam2, Encrypt, JobInput, JobInput2, JobOutput, JobOutput2, Transform}

/**
 * @param uuid    输入的uuid
 * @param idType  输入数据type
 * @param sep     分隔符
 * @param header  是否有header, 默认为0(无)
 * @param idx     多列数据的时候制定列标识, 给出列的序号,从1开始
 * @param headers 如果有header,该字段表示有哪些字段
 */

class DataEncryptionDecodingInputsV2(override val uuid: String, override val idType: Int = 0,
                                     override val sep: Option[String] = None, override val header: Int = 0,
                                     override val idx: Option[Int] = None,
                                     override val headers: Option[Seq[String]] = None,
                                     override val inputType: String = "")
  extends JobInput2(uuid = uuid, idType = idType, sep = sep, header = header, idx = idx, headers = headers,
    inputType = inputType) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, sep=$sep, header=$header, idx=$idx, headers=$headers
     """.stripMargin
}

/**
 * @param uuid       输出的uuid
 * @param hdfsOutput hdfs存储路径
 * @param limit      取前几行数据，默认全量数据
 * @param encryption 加解密标识，0：加密 1：解密
 * @param encrypt    加解密方式
 * @param transform  数据格式转换
 * @param keepSeed   是否保留种子包，0：不保留 1：保留 默认为0
 */

class DataEncryptionDecodingOutputV2(override val uuid: String, override val hdfsOutput: String = "",
                                     override val limit: Option[Int],
                                     val encryption: Int,
                                     val encrypt: Encrypt = Encrypt(0),
                                     override val keepSeed: Int = 1,
                                     val transform: Option[Map[String, Seq[String]]] = None)
  extends JobOutput2(uuid = uuid, hdfsOutput = hdfsOutput, limit = limit, keepSeed = keepSeed) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, encryption=$encryption, clean = ${transform.mkString(",")}
       |""".stripMargin
}

class DataEncryptionDecodingParamV2(override val inputs: Seq[DataEncryptionDecodingInputsV2],
                                    override val output: DataEncryptionDecodingOutputV2)
  extends BaseParam2(inputs, output) with Serializable {
  val outputEncrypt: Encrypt = output.encrypt
  val encryption: Int = output.encryption
  val transform: Transform = Transform(output.transform)
}

class DataEncryptionDecodingInputsV3(override val uuid: String, override val idType: Int = 0,
  override val sep: Option[String] = None, override val header: Int = 0,
  override val idx: Option[Int] = None,
  override val headers: Option[Seq[String]] = None,
  override val encrypt: Encrypt = Encrypt(0),
  override val inputType: String = "")
  extends JobInput2(uuid = uuid, idType = idType, sep = sep, header = header, idx = idx, headers = headers,
    inputType = inputType, encrypt = encrypt) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, sep=$sep, header=$header, idx=$idx, headers=$headers, encrypt=$encrypt
     """.stripMargin
}

class DataEncryptionDecodingParamV3(override val inputs: Seq[DataEncryptionDecodingInputsV3],
  override val output: DataEncryptionDecodingOutputV2)
  extends BaseParam2(inputs, output) with Serializable {
  val outputEncrypt: Encrypt = output.encrypt
  val encryption: Int = output.encryption
  val transform: Transform = Transform(output.transform)
}