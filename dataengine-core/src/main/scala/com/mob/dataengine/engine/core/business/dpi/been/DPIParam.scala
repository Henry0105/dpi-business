package com.mob.dataengine.engine.core.business.dpi.been

import java.io.File

import com.mob.dataengine.commons.{BaseParam2, JobInput2, JobOutput2}
import com.mob.dataengine.commons.enums.BusinessEnum
import com.mob.dataengine.commons.utils.{BusinessScriptUtils, JdbcTools}
import com.mob.dataengine.engine.core.business.dpi.helper.Jdbcs
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.jackson.JsonMethods._
import org.json4s._

/**
 * @param uuid     uuid
 * @param sep      分隔符,txt格式需要
 * @param value    http文件链接
 * @param url      dfs文件地址
 * @param carriers 运营商
 * @param catel1   分类
 * @param business 业务线   [[BusinessEnum]]
 */
case class DPIInput(override val uuid: String, override val sep: Option[String] = None,
                    value: String, url: String, carriers: Seq[String],
                    business: Option[Seq[String]] = None, catel1: Option[Int] = None)
  extends JobInput2(uuid = uuid, sep = sep) {

  override def toString: String =
    s"""
       |uuid=$uuid, carrier=${carriers.mkString(",")}, business=$business,
       |""".stripMargin

}

case class DPIOutput(override val uuid: String = "") extends JobOutput2(uuid = "")

class DPIParam(inputs: Seq[DPIInput], output: DPIOutput) extends BaseParam2(inputs, output) with Serializable {

  val srcTable = "src_table"
  val targetTable = "target_table"
  lazy val jdbcTools: JdbcTools = Jdbcs.of(BusinessEnum.dpi)
  var carrierInfos: Array[CarrierInfo] = _
  var tagInfos: Array[TagInfo] = _
  var tagInfoDF: DataFrame = _

  val value: String = inputs.head.value
  val url: String = inputs.head.url
  val version: String = inputs.head.uuid
  val carriers: Seq[String] = inputs.head.carriers.map(_.trim)
  val business: Option[Seq[Int]] = {
    inputs.head.business match {
      case Some(value) => Some(value.map(_.toInt))
      case None => None
    }
  }
  val catel1: Option[Int] = inputs.head.catel1
  val cbBean: DPICallBack = DPICallBack(version = version)

}

case class CarrierInfo(id: Int, name: String, genTagSql: String, preScreenSql: String, mpSql: String, genType: Int) {

  override def toString: String = {
    s"""
       |$id-$name =>
       |> genTagSql[${genType}]:$genTagSql
       |> preScreenSql:$preScreenSql
       |> mpSql:$mpSql
       |""".stripMargin
  }
}

case class TagInfo(carrierId: Int, shard: String,
                   tag: String, pattern: String, status: Int, userId: String, groupId: Int,
                   name: String, genType: Int)

/** url结构的样例类 */
case class UrlStruct(tag: String, url: String, urlRegexp: String, urlKey: String) {

  override def equals(obj: Any): Boolean = obj match {
    case that: UrlStruct => this.urlRegexp == that.urlRegexp && this.urlKey == that.urlKey
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()
}

/**
 * DPI的回调信息样例类
 *
 * @param errorCode      0:输入的URL不重复  1:输入的文件中URL重复 2:输入的URL历史重复
 * @param version        版本号
 * @param repetitiveTags 重复的tag map['unicom' -> array('new_tag_1,old_tag_1', 'new_tag_1,old_tag_2', ...)]
 */
case class DPICallBack(var errorCode: Int = 0, var version: String,
                       var repetitiveTags: Map[String, Seq[String]] = Map.empty[String, Seq[String]]) {

  def setError(errorCode: Int): Unit = {
    this.errorCode = errorCode
  }

  def setRepetitiveTags(carrier: String, repetitiveTags: Seq[String]): Unit = {
    this.repetitiveTags += (carrier -> repetitiveTags)
  }

  def toJson: String = {
    compact(render(Extraction.decompose(this)(org.json4s.DefaultFormats)).snakizeKeys)
  }
}