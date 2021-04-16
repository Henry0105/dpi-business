package com.mob.dataengine.core.bean

import com.mob.dataengine.core.bean.ApiURL._
import com.mob.dataengine.core.constants.DataengineExceptionType
import com.mob.dataengine.core.utils.{DataengineExceptionUtils}
import com.mob.dataengine.utils.PropUtils
import com.mob.library.network.http.Http
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, JField}


object ApiURL {
  lazy val API_QUERY_IOS_CONFIG: String = PropUtils.getProperty("api.query.ios_config")
}

case class HttpResponse[T](status: Int, message: String, content: T)

object HttpResponse {
  implicit val DFs: DefaultFormats = DefaultFormats
  private[this] val LOG = Logger.getLogger(this.getClass)

  def apply[T: Manifest](json: String): T = {
    val f: PartialFunction[JField, JField] = {
      case ("userid", x) => ("userId", x)
      case ("storeid", x) => ("storeId", x)
      case ("date", x) => ("beginDate", x)
    }
    val resp = JsonMethods.parse(json).transformField(f).extract[HttpResponse[T]]
    LOG.info(resp)
    if (resp.status != 200) {
      throw DataengineExceptionUtils(json, DataengineExceptionType.API_HTTP_ERROR)
    } else {
      resp.content
    }
  }

  def apply[T: Manifest](json: String, f: PartialFunction[JField, JField]): T = {
    val resp = JsonMethods.parse(json).transformField(f).extract[HttpResponse[T]]
    LOG.info(resp)
    if (resp.status != 200) {
      throw DataengineExceptionUtils(json, DataengineExceptionType.API_HTTP_ERROR)
    } else {
      resp.content
    }
  }
}


// IOS/安卓 比率表
case class IosConfig(
  keyName: String,
  typeName: String,
  iosRatio: Double,
  androidRatio: Double
)
object IosConfig {
  def apply(): Seq[IosConfig] = {
    val json = Http.get(API_QUERY_IOS_CONFIG)
    HttpResponse.apply[Seq[IosConfig]](json)
  }

  def apply(spark: SparkSession): DataFrame = {
    import spark.implicits._
    apply().toDF().selectExpr(
      "keyName as key_name", "typeName as type_name",
      "iosRatio as ios_ratio", "androidRatio as android_ratio"
    )
  }

}


