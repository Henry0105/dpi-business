package com.mob.dataengine.engine.core.profile.external

import com.google.gson.Gson
import org.scalatest.FunSuite
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._


class HttpAPITest extends FunSuite {
  val businessId = 9L
  val productId = "stats"
  val userId = 319220373160185856L
  val host = "exchange.appgo.cn"

  case class TagRelation(mobId: String, mobValue: String)
  case class CarrierProfileResponse(code: Int, data: Seq[TagRelation])
  case class CarrierProfileRequest(productId: String, userId: Long, bussinessId: Long)

  test("运营商画像性别") {
    val client = HttpClients.createDefault()

    val post = new HttpPost(s"http://$host/verification/gender")
    post.addHeader("Content-type", "application/json")
    post.addHeader("Content-Type", "charset=UTF-8")
    post.setEntity(new StringEntity(new Gson().toJson(Map("userId" -> userId,
      "productId" -> productId, "businessId" -> businessId,
      "req" -> Map("value" -> "18699476687", "paramType" -> "PHONE").asJava).asJava)))


    val response = client.execute(post)

    val resString = EntityUtils.toString(response.getEntity)
    println(resString)
    val js = JsonMethods.parse(resString)

    implicit val format = org.json4s.DefaultFormats

    val value = js \\ "tagRelation" \\ "mobValue"



    value.extract[String]

    println(js)
  }

  test("运营商画像年龄") {
    val client = HttpClients.createDefault()

    val post = new HttpPost(s"http://$host/verification/age")
    post.addHeader("Content-type", "application/json")
    post.addHeader("Content-Type", "charset=UTF-8")
    post.setEntity(new StringEntity(new Gson().toJson(Map("userId" -> userId,
      "productId" -> productId, "businessId" -> businessId,
      "req" -> Map("value" -> "18699001217", "paramType" -> "PHONE").asJava).asJava)))


    val response = client.execute(post)

    val resString = EntityUtils.toString(response.getEntity)
    println(resString)
    val js = JsonMethods.parse(resString)

    println(js)
  }

  test("个推画像") {
    val client = HttpClients.createDefault()

    val post = new HttpPost(s"http://$host/lable/getui")
    post.addHeader("Content-type", "application/json")
    post.addHeader("Content-Type", "charset=UTF-8")
    post.setEntity(new StringEntity(new Gson().toJson(Map("userId" -> userId,
      "productId" -> productId, "businessId" -> businessId,
      "req" -> Map("value" -> "e0aaad81bce9b5a3748b761103d399d6",
        "type" -> "IMEIMD5", "mobIds" -> "D004,D005,D010,D011").asJava).asJava)))


    val response = client.execute(post)

    val resString = EntityUtils.toString(response.getEntity)
    println(resString)
//    val js = JsonMethods.parse(resString)
//    println(js)
  }

  test("极光画像") {
    val client = HttpClients.createDefault()

    val post = new HttpPost(s"http://$host/lable")
    post.addHeader("Content-type", "application/json")
    post.addHeader("Content-Type", "charset=UTF-8")
    post.setEntity(new StringEntity(new Gson().toJson(Map("userId" -> "308970583652556800",
      "productId" -> "dataEngineWeb", "businessId" -> 1,
      "req" -> Map("value" -> "2107c35a35b5a47e8eeacff6e4c720ee", "type" -> "PHONEMD5").asJava).asJava)))


    val response = client.execute(post)

    val resString = EntityUtils.toString(response.getEntity)
    println(resString)
    val js = JsonMethods.parse(resString)

    for {
      JArray(arr) <- js \\ "tagRelation"
      JObject(obj) <- arr
    } {
      println(obj.toMap)
    }

//    println(js)
  }
}
