package com.mob.dataengine.commons.pojo

import scala.collection.mutable
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class OutCnt extends Serializable {
  val m = mutable.Map.empty[String, Long]

  def set(k: String, v: Long): Unit = {
    m.update(k, v)
  }

  def toJson(): JValue = {
    render(map2jvalue(m.toMap[String, Long]))
  }

  def toJsonString(): String = {
    compact(toJson())
  }
}

case class MatchInfo(jobId: String, uuid: String, idCnt: Long, matchCnt: Long, outCnt: OutCnt) {
  val m = mutable.Map.empty[String, Long]

  def set(k: String, v: Long): Unit = {
    m.update(k, v)
  }

  def toJsonString(): String = {
    val json =
      ("job_id" -> jobId) ~ ("uuid" -> uuid) ~ ("id_cnt" -> idCnt) ~
        ("match_cnt" -> matchCnt) ~ ("out_cnt" -> outCnt.toJson())
    compact(render(json).merge(render(map2jvalue(m.toMap[String, Long]))))
  }
}


case class MatchInfoV2(var idCnt: Long = 0L, var matchCnt: Long = 0L, var outCnt: OutCnt = new OutCnt, uuid: String) {
  val m = mutable.Map.empty[String, Long]

  def set(k: String, v: Long): Unit = {
    m.update(k, v)
  }

  def setMatchCnt(v: Long): Unit = {
    matchCnt = v
    // 兼容以前的输出结构
    outCnt.set("out_count", matchCnt)
  }

  def toJsonString(): String = {
    // 兼容以前的输出结构
    outCnt.set("out_count", matchCnt)

    val json =
      ("uuid" -> uuid) ~ ("id_cnt" -> idCnt) ~
        ("match_cnt" -> matchCnt) ~ ("out_cnt" -> outCnt.toJson())
    compact(render(json).merge(render(map2jvalue(m.toMap[String, Long]))))
  }
}