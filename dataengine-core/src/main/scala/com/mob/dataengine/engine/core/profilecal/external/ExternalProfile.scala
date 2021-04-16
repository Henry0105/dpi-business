package com.mob.dataengine.engine.core.profilecal.external

import com.google.gson.Gson
import com.mob.dataengine.commons.annotation.code.{explanation, sourceTable}
import com.mob.dataengine.commons.SeedSchema
import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.{BaseJob2, JobContext2}
import com.mob.dataengine.engine.core.jobsparam.profilecal.ExternalProfileParam
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{Column, DataFrame}
import org.json4s.JString
import org.json4s.JsonAST.{JArray, JObject, JValue}
import org.json4s.jackson.JsonMethods
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

abstract class ExternalProfile extends BaseJob2[ExternalProfileParam] {
  val mobId2ProfileMapping: Map[String, String] = MetadataUtils.findMobId2ProfileMapping()
  val profileId2MobIdMapping: Map[String, String] = mobId2ProfileMapping.map{ case (k, v) => (v, k)}
  var seedSchema: SeedSchema = _

  // 对应的渠道, 用于过滤hive表的分区
  def channel: String

  // 外部画像支持的标签ID
  def supportProfileIds: Seq[String]

  /**
   * @param ctx 任务上下文
   * */
  @explanation("创建 输入数据的 dataframe, source dataframe")
  @sourceTable("rp_dataengine_test.rp_data_hub")
  override def buildInputDataFrame(ctx: JobContext2[ExternalProfileParam]): DataFrame = {
    seedSchema = ctx.param.inputs.head.seedSchema
    ctx.hubService.readSeed(seedSchema)
  }

  override def persist2Hive(df: DataFrame, ctx: JobContext2[ExternalProfileParam]): Unit = {
    val idCntMap = ctx.hubService.statFeature(df, ctx.param.profileIds)
    idCntMap.foreach{ case (id, cnt) => ctx.matchInfo.outCnt.set(id, cnt)}

    ctx.hubService.writeToHub(df, ctx.param.output.uuid)
  }

  /**
   * @param df 输入DF
   * @param ctx 任务上下文
   * */
  @explanation("持久化到HDFS前需要判断 是否需要导出总之包 keepSeed：1 表示导出，0 表示不导出")
  override def transformSeedDF(df: DataFrame, ctx: JobContext2[ExternalProfileParam]): DataFrame = {
    import ctx.spark.implicits._
    // 将数据拼成tsv格式
    val ids = Seq("seed") ++ ctx.param.profileIds
    val idsBC = ctx.spark.sparkContext.broadcast(ids)
    ctx.spark.udf.register("df2tsv", combine2Tsv(_, idsBC.value))

    // 将id转为中文
    val code2CnMapping = ctx.sql(
      s"""
         |select code, cn
         |from cached_code_mapping
         |where code in (${ids.map(s => s"'$s'").mkString(",")})
        """.stripMargin)
      .map(r => (r.getAs[String]("code"), r.getAs[String]("cn")))
      .collect().toMap

    val header = ids.map{id => code2CnMapping.getOrElse(id, id)}.mkString("\t")
    // 这里直接从表里读数据, 由于缓存会被清理掉
    ctx.sql(
      s"""
         |select feature
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid='${ctx.param.output.uuid}'
        """.stripMargin).withColumn("tmp", callUDF("df2tsv", $"feature"))
      .select("tmp").toDF(header)
  }

  override def write2HDFS(df: DataFrame, ctx: JobContext2[ExternalProfileParam]): Unit = {
    ctx.hubService.writeToHDFS(df, ctx.param.output.hdfsOutput, Map("overwrite" -> "true",
      "source" -> "csv", "header" -> "true", "quote" -> "\u0000"))
  }

  def combine2Tsv(feature: Map[String, Seq[String]], ids: Seq[String]): String = {
    ids.map{ id =>
      val arr = feature.getOrElse(id, Seq("", ""))
      if (arr.length < 2) {
        arr.headOption.getOrElse("")
      } else {
        arr(1)
      }
    }.mkString("\t")
  }

  override def cacheInput: Boolean = true

  // 根据种子数据去查询画像,并合并已有的画像数据
  /**
   * @param df 输入的种子数据
   */
  override def transformData(df: DataFrame, ctx: JobContext2[ExternalProfileParam]): DataFrame = {
    import ctx.spark.implicits._

    df.createOrReplaceTempView("seed_tb")

    ctx.spark.udf.register("merge_profile", mergeProfile(_, _, ctx.jobCommon.day))
    val channelDay = try {
      ctx.getLastPar(PropUtils.HIVE_TABLE_EXT_LABEL_RELATION_FULL,
        0, s => s.split("/")(1).equals(s"channel=$channel")).split("=")(1)
    } catch {
      case ex: Throwable => ex.printStackTrace()
        "empty_day"
    }

    // 离线合并好的外部画像表
    ctx.sql(
      s"""
         |select value[${ctx.param.inputEncrypt.encryptType}] id, label as profile
         |from ${PropUtils.HIVE_TABLE_EXT_LABEL_RELATION_FULL}
         |where day = '$channelDay' and channel = '$channel'
        """.stripMargin).createOrReplaceTempView("profile_tb")

    val profileValueMap = MetadataUtils.findProfileValue(ctx.param.profileIds)
    val profileValueMapBC = ctx.spark.sparkContext.broadcast(profileValueMap)

    ctx.sql(
      s"""
         |select a.id, merge_profile(feature, profile) feature
         |from seed_tb as a
         |left join profile_tb as b
         |on a.id = b.id
        """.stripMargin)
      .repartition(Math.max(1, ctx.matchInfo.idCnt.toInt/100)).mapPartitions{ it =>
        val timeout = 20
        val config = RequestConfig.custom()
          .setConnectTimeout(timeout * 1000)
          .setConnectionRequestTimeout(timeout * 1000)
          .setSocketTimeout(timeout * 1000).build()
        val httpClient: CloseableHttpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build()
        val feature = it.toArray.map{ r =>
          val idValue = r.getString(0)
          val feature = r.getAs[Map[String, Seq[String]]]("feature")
          // 查询外部画像
          val httpProfile = queryProfileByAPI(ctx.param, idValue, feature, httpClient)
          trans2Cn(unifyProfile(httpProfile, ctx.jobCommon.day) ++ feature, profileValueMapBC.value)
        }
        httpClient.close()
        feature.iterator
      }.toDF("feature")
  }

  override def createMatchIdsCol(ctx: JobContext2[ExternalProfileParam]): Seq[Column] = null


  /**
   * @param seedProfile 种子包的的画像数据: profile_id -> [day, profile_value]
   * @param extProfile  外部画像数据: profile_id -> profile_value
   * 种子包已有的画像不被覆盖
   * return: profile_id -> [day, profile_value]
   */
  def mergeProfile(seedProfile: Map[String, Seq[String]],
    extProfile: Map[String, String], day: String): Map[String, Seq[String]] = {
    val res: Map[String, Seq[String]] = if (null == extProfile || extProfile.isEmpty) {
      seedProfile
    } else if (null == seedProfile || seedProfile.isEmpty) {
      unifyProfile(extProfile, day)
    } else {
      unifyProfile(extProfile, day) ++ seedProfile
    }

    if (null == res || res.isEmpty) {
      null
    } else {
      res
    }
  }

  // 给原始的profile数据加个日期进去，符合rp_data_hub结构的设定
  def unifyProfile(profile: Map[String, String], day: String): Map[String, Seq[String]] = {
    if (null == profile || profile.isEmpty) {
      Map.empty[String, Seq[String]]
    } else {
      profile.map{ case (k, s) => (k, Seq(day, s))}
    }
  }

  /**
   * @param feature: profile -> [day, value]
   * @param profileValueMap: 中文对照表 profile_version_id_value -> cn
   */
  def trans2Cn(feature: Map[String, Seq[String]], profileValueMap: Map[String, String]): Map[String, Seq[String]] = {
    feature.map{ case (k, seq) =>
      if (seq.length < 2) {
        k -> seq
      } else {  // 这里长度只能为2
        k -> Seq(seq.head, profileValueMap.getOrElse(s"${k}_${seq(1)}", seq(1)))
      }
    }
  }

  // 找出对应没有值的profileid
  def filterEmptyMobIds(feature: Map[String, Seq[String]], param: ExternalProfileParam): Seq[String] = {
    val profileIds = param.profileIds.intersect(supportProfileIds)
    // 查出为空的mobId进行处理
    val emptyProfileIds = profileIds.filter{p =>
      val seq = feature.getOrElse(p, Seq.empty[String])
      seq.length < 2 || StringUtils.isBlank(seq(1))
    }
    profileIds2MobId(emptyProfileIds)
  }

  /**
   * 访问外部交换的api, 拿到对应的profile_id -> value的对应关系
   * @param param 任务参数
   * @param idValue id的值
   * @param feature 该Id对应的已有的画像
   */
  def queryProfileByAPI(param: ExternalProfileParam, idValue: String,
    feature: Map[String, Seq[String]], httpClient: HttpClient): Map[String, String]


  def buildPost(url: String, param: ExternalProfileParam, idValue: String, paramTypeField: String): HttpPost = {
    val post = new HttpPost(url)
    post.addHeader("Content-type", "application/json")
    post.addHeader("Content-Type", "charset=UTF-8")
    post.setEntity(new StringEntity(new Gson().toJson(Map("userId" -> param.userId,
      "productId" -> param.productId, "businessId" -> param.businessId,
      "req" -> Map("value" -> idValue, paramTypeField -> param.paramType).asJava).asJava)))
    post
  }

  def postRequest(httpClient: HttpClient, post: HttpPost): HttpResponse = {
    httpClient.execute(post)
  }

  def parseResponse(response: HttpResponse): Map[String, String] = {
    val resString = EntityUtils.toString(response.getEntity)

    val js = JsonMethods.parse(resString)
    val res = (for {
      JArray(list) <- js \\ "tagRelation"
      JObject(listObject) <- list
    } yield {
      // Map(mobId -> JString(7012_001), channelValueDesc -> JString(打车类应用偏好),
      // mobValue -> JString(1.0E-4), channelValue -> JString(1.0E-4),
      // channelId -> JString(APP_HOBY_TAXI))
      val m = listObject.toMap
      if (m.contains("mobId")) {
        (parseJString(m("mobId")), parseJString(m("mobValue")))
      } else {
        null
      }
    }).filter(_ != null)
      .toMap

    if (res.nonEmpty) {
      // 将mobId转为profileId
      res.map{ case (mobId, mobValue) => (mobId2ProfileMapping(mobId), mobValue)}
    } else {
      Map.empty[String, String]
    }
  }

  def parseJString(js: JValue): String = {
    js match {
      case JString(s) => s
      case _ => null
    }
  }

  // 将profile转为mobid
  def profileIds2MobId(profileIds: Seq[String]): Seq[String] = {
    if (null == profileIds || profileIds.isEmpty) {
      Seq.empty[String]
    } else {
      profileId2MobIdMapping.filterKeys(profileIds.contains).values.toSeq
    }
  }

  // 将mobid转为profileId
  def mobId2ProfileId(mobIds: Seq[String]): Seq[String] = {
    if (null == mobIds || mobIds.isEmpty) {
      Seq.empty[String]
    } else {
      mobId2ProfileMapping.filterKeys(mobIds.toSet.contains).values.toSeq
    }
  }
}
