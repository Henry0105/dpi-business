package com.mob.dataengine.engine.core.jobsparam.profile.export.bt

import com.mob.dataengine.commons.{BaseParam, JobInput, JobOutput}
import com.mob.dataengine.commons.enums.{InputType => InputTypeEnum}

/**
 * 单体任务输入参数
 * @param uuid             输入数据的uuid分区 或者 sql语句
 * @param profileIds       标签列表
 * @param minDay           回溯结束日期
 * @param trackDay         回溯开始日期
 * @param trackDayIndex    回溯开始日期索引(以1开始)
 * @param idx              使用sql的时候需要指定 device的列数 以1开始
 * @param sep              使用sql的时候需要制定拼接分隔符
 * @param inputType        输入类型[[InputTypeEnum]]
 */
class ProfileBatchBackTrackerInput(override val uuid: String, val profileIds: Array[String],
  val minDay: Option[String] = None, val trackDay: Option[String] = None, val trackDayIndex: Option[Int] = None,
  override val idx: Option[Int] = None,
  override val sep: Option[String] = None, override val inputType: String)
  extends JobInput(uuid, 4, idx, sep, inputType) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, profileIds=${profileIds.mkString(",")}, minDay=$minDay, trackDay=$trackDay,
       |trackDayIndex = $trackDayIndex
     """.stripMargin
}

class ProfileBatchBackTrackerOutput(override val uuid: String, override val hdfsOutput: String,
  val limit: Option[Int] = None, val display: Int = 0)
  extends JobOutput(uuid, hdfsOutput) with Serializable {
  val outputHDFSLanguage: String = if (display == 1) "cn" else "en"
  override def toString: String =
    s"""
       |${super.toString}, outputHDFSLanguage=$outputHDFSLanguage
     """.stripMargin
}

class ProfileBatchBackTrackerParam(override val inputs: Seq[ProfileBatchBackTrackerInput],
  override val output: ProfileBatchBackTrackerOutput)
  extends BaseParam(inputs, output) with Serializable {

}

