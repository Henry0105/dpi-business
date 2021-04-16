package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{BaseParam2, JobInput2, JobOutput2}

/** http://c.mob.com/pages/viewpage.action?pageId=26680032 */
class PidOperatorMappingInput(override val uuid: String, override val idType: Int = 3,
                                override val sep: Option[String] = None, override val header: Int = 0,
                                override val idx: Option[Int] = None, override val inputType: String = "uuid",
                                override val headers: Option[Seq[String]] = Some(Seq("phone")),
                                val include: Map[String, String] = null, val exclude: Map[String, String] = null)
  extends JobInput2(uuid = uuid, idType = idType, sep = sep, idx = idx, inputType = inputType,
    header = header, headers = headers) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, sep=$sep, header=$header, idx=$idx,
       |headers=$headers, include=$include, exclude=$exclude
     """.stripMargin
}

class PidOperatorMappingOutput(override val uuid: String, override val hdfsOutput: String)
  extends JobOutput2(uuid, hdfsOutput) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, uuid=$uuid, limit=$limit
     """.stripMargin
}

class PidOperatorMappingParam(override val inputs: Seq[PidOperatorMappingInput],
                                   override val output: PidOperatorMappingOutput)
  extends BaseParam2(inputs, output) with Serializable {
  val include: Map[String, String] = inputs.head.include
  val exclude: Map[String, String] = inputs.head.exclude
}
