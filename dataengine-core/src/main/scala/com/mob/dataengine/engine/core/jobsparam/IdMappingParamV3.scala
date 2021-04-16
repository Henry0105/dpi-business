package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{BaseParam, Encrypt, JobInput, JobOutput, SeedSchema}
import com.mob.dataengine.commons.enums.DeviceType
import com.mob.dataengine.commons.enums.DeviceType.{DeviceType, IMEI14, IMEI15, IMEI}
import com.mob.dataengine.commons.{BaseParam, Encrypt, JobInput, JobOutput}
import com.mob.dataengine.commons.enums.{InputType => InputTypeEnum}

/**
 * @param uuid          输入数据的uuid分区
 * @param idType        输入数据的类型,imei/mac/phone/device/imei14
 * @param encrypt       加密方式
 * @param sep           多列数据的时候, 指定分割符号
 * @param header        是否有header, 默认为0(无)
 * @param idx           多列数据的时候制定列标识, 给出列的序号,从1开始
 * @param headers       如果有header,该字段表示有哪些字段
 * @param cleanImei     0 表示不过滤,1表示过滤掉带字母的imei
 * @param deviceMatch   0表示默认方式,1表示画像最全,2表示回溯匹配
 * @param trackDay      回溯时间为可空字段，若为空，则采用种子包中的时间列；若有值，则种子包中的所有数据全部采用此时间点进行回溯
 * @param trackDayIndex 回溯开始日期索引(以1开始),默认为2
 * @param inputType     输入类型[[InputTypeEnum]]
 */
class IdMappingV3Input(override val uuid: String, override val idType: Int = 4,
                       override val encrypt: Encrypt = Encrypt(0),
                       override val sep: Option[String] = None, val header: Int = 0,
                       override val idx: Option[Int] = None,
  val headers: Option[Seq[String]] = None, val cleanImei: Int = 0, val deviceMatch: Int = 0,
  val trackDay: Option[String] = None, val trackDayIndex: Option[Int] = None,
  override val inputType: String = "uuid")
  extends JobInput(uuid = uuid, idType = idType, idx = idx, sep = sep, inputType = inputType, encrypt = encrypt)
    with Serializable {

  def seedSchema: SeedSchema = SeedSchema(InputTypeEnum.withName(inputType), idType,
    sep.getOrElse(","), headers.getOrElse(Seq.empty[String]), idx.getOrElse(1), uuid, encrypt)

  override def toString: String =
    s"""
       |${super.toString}, encrypt=$encrypt, sep=$sep, header=$header, idx=$idx,
       |headers=$headers, cleanImei=$cleanImei, deviceMatch=$deviceMatch,
       |trackDay=$trackDay, trackDayIndex=$trackDayIndex, inputType=$inputType
     """.stripMargin
}

/**
 * idmappingv2 输出参数
 *
 * @param uuid          输出的uuid
 * @param hdfsOutput    输出的hdfs目录,内部参数
 * @param idTypes       输出的id
 * @param encrypt       输出加密参数
 * @param limit         输出限制,-1为不限制
 * @param matchLimit    匹配到多条的时候选择匹配条数,-1表示匹配全部
 * @param matchOrigImei 是否匹配主卡imei，0表示不匹配，1表示匹配
 * @param keepSeed      0表示导出的时候不保留种子数据,并且将数据都放在一列中；默认1，不做处理
 */
class IdMappingV3Output(override val uuid: String, override val hdfsOutput: String,
                        val idTypes: Seq[Int], val encrypt: Encrypt = Encrypt(0), val limit: Int = -1,
                        val matchLimit: Option[Int] = Some(1), val matchOrigImei: Option[Int] = Some(0),
                        val keepSeed: Option[Int] = Some(1))
  extends JobOutput(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, idTypes=${idTypes.mkString(",")}, encrypt=$encrypt, limit=$limit,
       | matchLimit=$matchLimit, matchorigimei=$matchOrigImei, keepSeed=$keepSeed
     """.stripMargin

  def idTypesEnum: Seq[DeviceType] = idTypes.map(DeviceType(_))
}

class IdMappingV3Param(override val inputs: Seq[IdMappingV3Input], override val output: IdMappingV3Output)
  extends BaseParam(inputs, output) with Serializable {
  val inputIdTypeEnum: DeviceType = inputs.head.idTypeEnum
  val sep: Option[String] = inputs.head.sep
  val idx: Option[Int] = inputs.head.idx
  val header: Int = inputs.head.header
  var headers: Option[Seq[String]] = inputs.head.headers

  lazy val origJoinField: String = if (header > 0) {
    headers.get(idx.get - 1)
  } else {
    "in_id"
  }
  // 表示种子表的join的列
  val leftField: String = "left_field"
  // 表示mapping表join的列
  val rightField: String = "right_field"
  val inputEncrypt: Encrypt = inputs.head.encrypt
  // 表示mapping表是不是imei表
  val imeiKeys: Set[DeviceType] = Set(IMEI14, IMEI15, IMEI)
  val isImeiTable: Boolean = imeiKeys.contains(inputIdTypeEnum)
  val cleanImei: Int = inputs.head.cleanImei
  val deviceMatch: Int = inputs.head.deviceMatch
  val trackDay: Option[String] = inputs.head.trackDay
  val trackDayIndex: Option[Int] = inputs.head.trackDayIndex
  val inputTypeEnum = inputs.head.inputTypeEnum
}
