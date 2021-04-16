package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{JobInput, JobOutput}


/**
 * 人群交并差任务参数
 *
 * */
class CrowdV2Input (override val uuid: String, override val inputType: String, val name: Option[String]
                    , idType: Int = 4)
  extends JobInput ( uuid, idType = idType) with Serializable {
  override def toString: String = {
    s"""
       | uuid = $uuid,
       | inputType = $inputType
       | name = ${name.get}
     """.stripMargin
  }
}

class CrowdV2Output (override val uuid: String,
                     override val hdfsOutput: String = "",
                     val opts: String,
                     val limit: Option[Int])
  extends JobOutput (uuid, hdfsOutput = "") with Serializable {
  override def toString: String = {
    s"""
       |uuid: $uuid
       |opts: $opts
       |hdfsOutput: $hdfsOutput
   """.stripMargin
  }
}
class CrowdSetParamV2(val inputs: Seq[CrowdV2Input], val output: CrowdV2Output) {

}
