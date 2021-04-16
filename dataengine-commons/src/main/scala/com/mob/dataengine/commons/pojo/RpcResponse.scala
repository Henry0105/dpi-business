package com.mob.dataengine.commons.pojo

import com.mob.dataengine.commons.enums.JobName.JobName
import org.json4s.DefaultFormats

import scala.collection.mutable

/**
 * @author juntao zhang
 */
case class RpcResponse(
  respType: JobName,
  code: Int,
  msg: String,
  applicationId: String,
  data: mutable.HashMap[String, Any] = new mutable.HashMap[String, Any]()
) {
  def put(k: String, v: Any): RpcResponse = {
    data.put(k, v)
    this
  }
}

object RpcResponse {

  import org.json4s.jackson.Serialization._

  implicit val formats: DefaultFormats.type = DefaultFormats

  def mkString(r: RpcResponse): String = writePretty(r)
}
