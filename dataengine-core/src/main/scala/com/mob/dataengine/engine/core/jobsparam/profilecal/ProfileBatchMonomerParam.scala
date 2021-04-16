package com.mob.dataengine.engine.core.jobsparam.profilecal

import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.{BaseParam, Encrypt, JobInput, JobOutput}
import com.mob.dataengine.commons.enums.{InputType => InputTypeEnum}


/**
 * 单体任务输入参数
 * @param uuid        输入数据的uuid分区 或者 sql语句
 * @param idType      输入类型
 * @param profileIds  标签列表
 * @param filter      过滤条件
 * @param idx         使用sql的时候需要指定 device的列数 以1开始
 * @param sep         使用sql的时候需要制定拼接分隔符 默认 "\u0001"
 * @param inputType   输入类型[[InputTypeEnum]]
 * @param encrypt     加密方式
 */
class ProfileBatchMonomerInput(override val uuid: String, val profileIds: Seq[String],
  val filter: Option[Map[String, String]] = None,
  override val idx: Option[Int] = None, override val sep: Option[String] = None,
  override val inputType: String, override val idType: Int = 4, override val encrypt: Encrypt = Encrypt(0))
  extends JobInput(uuid, idType, idx, sep, inputType, encrypt) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, profileIds=${profileIds.mkString(",")}, filter=$filter
     """.stripMargin
}

/**
 *
 * @param uuid        输出的uuid分区
 * @param hdfsOutput  输出hdfs地址
 * @param display     输出标签时输出原始值还是转成中文的值  =0 表示原始值  =1表示转成中文输出
 * @param limit       输出行数限制
 * @param order       排序规则
 * @param keepSeed    0表示导出的时候不保留种子数据,并且将数据都放在一列中；默认1，不做处理
 */
class ProfileBatchMonomerOutput(override val uuid: String, override val hdfsOutput: String,
  val display: Int, val limit: Option[Int], val order: Option[Seq[Map[String, Any]]] = None,
  val keepSeed: Option[Int] = Some(1))
  extends JobOutput(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, display=$display, limit=$limit
     """.stripMargin
}

class ProfileBatchMonomerParam(override val inputs: Seq[ProfileBatchMonomerInput],
  override val output: ProfileBatchMonomerOutput)
  extends BaseParam(inputs, output) with Serializable {

  val profileIds: Seq[String] = inputs.flatMap(_.profileIds).distinct
  val inputIdTypeEnum: DeviceType = inputs.head.idTypeEnum
  val encrypt: Encrypt = inputs.head.encrypt

}

