package com.mob.dataengine.engine.core.profilecal.external

import com.mob.dataengine.commons.utils.AppUtils
import com.mob.dataengine.engine.core.jobsparam.profilecal.ExternalProfileParam
import org.apache.http.client.HttpClient


/**
 * 运营商画像
 */

object CarrierProfile extends ExternalProfile {
  /**
   * 调用运营商接口
   * 只有性别和年龄2个标签
   */

  val path = s"http://${AppUtils.EXTERNAL_PROFILE_HOST}/verification"
  val mobId2ProfileIdMap: Map[String, String] = Map("D004" -> "1_1000", "D005" -> "2_1000")
  val mobId2PathMap: Map[String, String] = Map("D004" -> "gender", "D005" -> "age")

  override def channel: String = "guangdongUnicom"
  override def supportProfileIds: Seq[String] = mobId2ProfileIdMap.values.toSeq

  override def queryProfileByAPI(param: ExternalProfileParam, idValue: String,
    feature: Map[String, Seq[String]], httpClient: HttpClient): Map[String, String] = {

    val _res = filterEmptyMobIds(feature, param)
      .map { mobId =>
      val url = s"$path/${mobId2PathMap(mobId)}"
      val post = buildPost(url, param, idValue, "paramType")
      val response = postRequest(httpClient, post)
      parseResponse(response)
    }

    _res.filter(_ != null) match {
      case Nil => null
      case xs => xs.reduce(_ ++ _)
    }
  }
}
