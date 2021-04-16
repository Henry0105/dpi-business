package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{BaseParam, JobInput, JobOutput}
import org.apache.commons.lang3.StringUtils

class LocationDeviceMappingParamInput(val areaType: Int, val dailyOrMonthly: Int, val areaCode: Option[String],
  val cityCode: Option[String], val latLonList: Option[String],
  val beginDay: Option[String], val endDay: Option[String])
  extends JobInput("", 4) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, areaType=$areaType, dailyOrMonthly=$dailyOrMonthly, areaCode=$areaCode
       |cityCode=$cityCode, latLonList=$latLonList, beginDay=$beginDay, endDay=$endDay, locationCode=$locationCode
     """.stripMargin

  val locationCode: Option[String] = areaCode match {
    case Some(c) if StringUtils.isNotBlank(c) => areaCode
    case _ => cityCode
  }
}

class LocationDeviceMappingParam(override val inputs: Seq[LocationDeviceMappingParamInput],
  override val output: JobOutput)extends BaseParam(inputs, output) with Serializable {
  val input: LocationDeviceMappingParamInput = inputs.head
}
