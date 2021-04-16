package com.mob.dataengine.engine.core.jobsparam.profilecal

import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.{BaseParam2, Encrypt, JobInput2, JobOutput2, SeedSchema}

/**
 * 外部画像任务的参数
 */

/**
 * @param uuid    输入数据的uuid分区
 * @param idType  输入数据的类型,imei/mac/phone/device/imei14
 * @param encrypt 加密方式
 * @param sep     多列数据的时候, 指定分割符号
 * @param header  是否有header, 默认为0(无)
 * @param idx     多列数据的时候制定列标识, 给出列的序号,从1开始
 * @param headers 如果有header,该字段表示有哪些字段
 * @param accountId  用户ID
 * @param productId 产品ID
 * @param businessId 业务线ID
 * @param profileIds 标签ID
 */

class ExternalProfileInput(override val uuid: String, override val idType: Int = 4, override val inputType: String,
  override val encrypt: Encrypt = Encrypt(0), override val sep: Option[String] = None, override val header: Int = 0,
  override val idx: Option[Int] = None, override val headers: Option[Seq[String]] = None,
  val accountId: Long, val productId: String, val businessId: Long, val profileIds: Seq[String])
  extends JobInput2(uuid = uuid, idType = idType, inputType = inputType,
    encrypt = encrypt, sep = sep, header = header, headers = headers, idx = idx) with Serializable {

  override def toString: String =
    s"""
       |${super.toString}, encrypt=$encrypt, sep=$sep, header=$header, idx=$idx,
       |headers=$headers, accountId=$accountId, productId=$productId, bussinessId=$businessId,
       |profileIds=${profileIds.mkString(",")}
     """.stripMargin
}

class ExternalProfileOutput(override val uuid: String, override val hdfsOutput: String,
  override val limit: Option[Int] = Some(-1))
  extends JobOutput2(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString} limit=$limit
     """.stripMargin
}

class ExternalProfileParam(override val inputs: Seq[ExternalProfileInput], override val output: ExternalProfileOutput)
  extends BaseParam2(inputs, output) with Serializable {
  // 表示种子表的join的列
  val leftField: String = "left_field"
  // 表示mapping表join的列
  val rightField: String = "right_field"
  val userId: Long = inputs.head.accountId
  val businessId: Long = inputs.head.businessId
  val productId: String = inputs.head.productId
  val profileIds: Seq[String] = inputs.head.profileIds
  val paramType: String = getParamType(inputIdTypeEnum, inputEncrypt)

  /**
   * 根据id类型和加密类型,获取外部交换接口的paramType,都使用大写
   */
  def getParamType(idType: DeviceType, encrypt: Encrypt): String = {
    val suffix = encrypt.encryptType match {
      case 1 => "MD5"
      case 4 => "SHA256"
      case _ => ""
    }
    idType.toString.toUpperCase().replace("_", "") + suffix
  }
}
