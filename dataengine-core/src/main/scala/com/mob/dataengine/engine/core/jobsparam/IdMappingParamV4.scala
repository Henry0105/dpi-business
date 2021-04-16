package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.enums.SecDeviceType.{SecDeviceType, IEID, IEID14, IEID15}
import com.mob.dataengine.commons.enums.{SecDeviceType, InputType => InputTypeEnum}
import com.mob.dataengine.commons.{BaseParam2, Encrypt, JobInput2, JobOutput2}

/**
 * @param uuid          输入数据的uuid分区
 * @param idType        输入数据的类型,ieid/mcid/pid/device
 * @param encrypt       加密方式
 * @param sep           多列数据的时候, 指定分割符号
 * @param idx           多列数据的时候制定列标识, 给出列的序号,从1开始
 * @param cleanIeid     0 表示不过滤,1表示过滤掉带字母的ieid
 * @param deviceMatch   0表示默认方式,1表示画像最全,2表示回溯匹配
 * @param trackDay      回溯时间为可空字段，若为空，则采用种子包中的时间列；若有值，则种子包中的所有数据全部采用此时间点进行回溯
 * @param trackDayIndex 回溯开始日期索引(以1开始),默认为2
 * @param inputType     输入类型[[InputTypeEnum]]
 */
class IdMappingV4Input(override val uuid: String, override val idType: Int = 4,
                       override val encrypt: Encrypt = Encrypt(0),
                       override val sep: Option[String] = None, override val header: Int = 0,
  override val headers: Option[Seq[String]] = None, override val idx: Option[Int] = None,
  val cleanIeid: Int = 0, val deviceMatch: Int = 0,
  val trackDay: Option[String] = None, val trackDayIndex: Option[Int] = None,
  override val inputType: String = "uuid")
  extends JobInput2(uuid = uuid, idType = idType, idx = idx, sep = sep, inputType = inputType, encrypt = encrypt,
    header = header, headers = headers)
    with Serializable {

  override def toString: String =
    s"""
       |${super.toString}, encrypt=$encrypt, sep=$sep, header=$header, idx=$idx,
       |headers=$headers, cleanImei=$cleanIeid, deviceMatch=$deviceMatch,
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
 * @param matchLimit    匹配到多条的时候选择匹配条数,-1表示匹配全部
 * @param matchOrigIeid 是否匹配主卡imei，0表示不匹配，1表示匹配
 */
class IdMappingV4Output(override val uuid: String, override val hdfsOutput: String,
                        val idTypes: Seq[Int], val encrypt: Encrypt = Encrypt(0),
  override val limit: Option[Int] = Some(-1), override val keepSeed: Int = 1,
  val matchLimit: Option[Int] = Some(1), val matchOrigIeid: Option[Int] = Some(0))
  extends JobOutput2(uuid, hdfsOutput, limit = limit, keepSeed = keepSeed) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, idTypes=${idTypes.mkString(",")}, encrypt=$encrypt, limit=$limit,
       | matchLimit=$matchLimit, matchorigieid=$matchOrigIeid, keepSeed=$keepSeed
     """.stripMargin

  def idTypesEnumSec: Seq[SecDeviceType] = idTypes.map(SecDeviceType(_))
}

class IdMappingV4Param(override val inputs: Seq[IdMappingV4Input], override val output: IdMappingV4Output)
  extends BaseParam2(inputs, output) with Serializable {

  // 表示种子表的join的列
  val leftField: String = "left_field"
  // 表示mapping表join的列
  val rightField: String = "right_field"
  // 表示mapping表是不是imei表
  val ieidKeys: Set[SecDeviceType] = Set(IEID14, IEID15, IEID)
  val isIeidTable: Boolean = ieidKeys.contains(inputIdTypeEnumSec)
  val cleanIeid: Int = inputs.head.cleanIeid
  val deviceMatch: Int = inputs.head.deviceMatch
  val trackDay: Option[String] = inputs.head.trackDay
  val trackDayIndex: Option[Int] = inputs.head.trackDayIndex
}
