package com.mob.dataengine.engine.core.profilecal.external
import com.google.gson.Gson
import com.mob.dataengine.commons.utils.AppUtils
import com.mob.dataengine.engine.core.jobsparam.profilecal.ExternalProfileParam
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object GeTuiProfile extends ExternalProfile {

  override def channel: String = "getui"
  override def supportProfileIds: Seq[String] = mobId2ProfileId(AppUtils.GETUI_PROFILEIDS.split(","))

  /**
   * 访问外部交换的api, 拿到对应的profile_id -> value的对应关系
   * @param param 任务参数
   * @param idValue id的值
   * @param feature 该Id对应的已有的画像
   */
  override def queryProfileByAPI(param: ExternalProfileParam, idValue: String,
    feature: Map[String, Seq[String]], httpClient: HttpClient): Map[String, String] = {
    val url = s"http://${AppUtils.EXTERNAL_PROFILE_HOST}/lable/getui"
    val post = buildPost(url, param, idValue, "type")
    val mobIds = filterEmptyMobIds(feature, param)
    post.setEntity(new StringEntity(new Gson().toJson(Map("userId" -> param.userId,
      "productId" -> param.productId, "businessId" -> param.businessId,
      "req" -> Map("value" -> idValue, "type" -> param.paramType, "mobIds" -> mobIds.mkString(","),
      "accountId" -> "1").asJava).asJava)))

    val response = postRequest(httpClient, post)
    // 接口可能会查询3个, 返回8个, (目前个推接口最全会返回8个), 需要过滤一下
    parseResponse(response).filterKeys(param.profileIds.toSet.contains)
  }
}
