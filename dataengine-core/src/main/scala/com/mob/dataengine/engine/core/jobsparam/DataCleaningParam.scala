package com.mob.dataengine.engine.core.jobsparam


import com.mob.dataengine.commons._

class DataCleaningInputs(override val uuid: String, override val idType: Int = 0,
                         override val sep: Option[String] = None, override val header: Int = 0,
                         override val idx: Option[Int] = None, override val headers: Option[Seq[String]] = None)
  extends JobInput2 (uuid, idType, sep, header, idx, headers) with Serializable {

}
/**
 *
 * @param regexpClean 正则表达式
 * @param transform 转换列表
 * @param cleanImei 去掉带字母的imei 默认0 不去掉
 * @param keepSeed 是否保留种子包数据 默认0 不保留
 *
 * */
class DataCleaningOutput(override val uuid: String, override val hdfsOutput: String = "",
                         override val limit: Option[Int], val regexpClean: Option[String] = None,
                         val transform: Option[Map[String, Seq[String]]] = None, val cleanImei: Int = 0,
                         override val keepSeed: Int = 1)
  extends JobOutput2 (uuid, hdfsOutput = "", limit, keepSeed) with Serializable {
  override def toString: String =
    s"""
       |uuid = $uuid,
       |hdfsOutput = $hdfsOutput,
       |limit = $limit,
       |regexpClean = $regexpClean,
       |transform = $transform,
       |cleanImei = $cleanImei,
       |keepSeed = $keepSeed
       |
     """.stripMargin
}

class DataCleaningParam(override val inputs: Seq[DataCleaningInputs],
                        override val output: DataCleaningOutput)
  extends BaseParam2(inputs, output) with Serializable {


}

