package com.mob.dataengine.engine.core.profilecal.external

import com.mob.dataengine.commons.utils.AppUtils
import com.mob.dataengine.engine.core.jobsparam.profilecal.ExternalProfileParam
import org.apache.http.client.HttpClient

object JiGuangProfile extends ExternalProfile {
  override def channel: String = "aurora"
  override def supportProfileIds: Seq[String] = mobId2ProfileId(AppUtils.GIGUANG_PROFILEIDS.split(","))

  /**
   * 访问外部交换的api, 拿到对应的profile_id -> value的对应关系
 *
   * @param param 任务参数
   * @param idValue id的值
   * @param feature 该Id对应的已有的画像
   */
  override def queryProfileByAPI(param: ExternalProfileParam, idValue: String, feature: Map[String, Seq[String]],
    httpClient: HttpClient): Map[String, String] = {

    val url = s"http://${AppUtils.EXTERNAL_PROFILE_HOST}/lable"
    val post = buildPost(url, param, idValue, "type")
    val response = postRequest(httpClient, post)
    parseResponse(response).filterKeys(param.profileIds.toSet.contains)
  }
}