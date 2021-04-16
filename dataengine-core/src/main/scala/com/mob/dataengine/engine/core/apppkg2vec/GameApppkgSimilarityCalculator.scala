package com.mob.dataengine.engine.core.apppkg2vec

import com.mob.dataengine.commons.enums.Apppkg2VecType
import com.mob.dataengine.commons.traits.{Cacheable, TableTrait}
import com.mob.dataengine.commons.utils.PropUtils._
import com.mob.dataengine.engine.core.jobsparam.{Apppkg2VecParam, BaseJob}
import com.mob.dataengine.rpc.RpcClient
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * sh ../sbin/dataengine-submit.sh \
 * 'http://10.18.97.72:20101/fs/download?path=apppkg_similarity_example_001&module=dataengine' 1
 *
 * 竞品单次计算任务, 根据输入的appName和其它app之间计算相似度, 根据limit参数取topN的appName输出
 * 一共三个App相关的特征表, 几种情况:
 * 1. app2vec中未查到, 直接返回空
 * 2. app2vec查到, icon2vec和detail2vec未查到, 仅返回top[app2vec]
 * 3. app2vec和icon2vec查到, detail2vec未查到, 返回top[app2vec], top[icon2vec], top[app2vec加权icon2vec]
 * 4. app2vec和detail2vec查到, icon2vec未查到, 返回top[app2vec], top[detail2vec], top[app2vec加权detail2vec]
 * 5. 三个特征全部查到, 返回top[app2vec], top[icon2vec], top[detail2vec], top[app2vec加权icon2vec加权detail2vec]
 *
 * 任务耗时: 2min
 */
object GameApppkgSimilarityCalculator extends BaseJob[Apppkg2VecParam] with Cacheable with TableTrait {

  @transient lazy override implicit val spark: SparkSession = jobContext.spark
  import spark.implicits._
  import spark.{sparkContext => sc}
  lazy val appName: String = jobContext.params.head.inputs.head.appName
  lazy val limit: Int = jobContext.params.head.output.limit


//  def lastPar(table: String): String = spark.sql(s"show partitions $table")
//    .collect().map(_.getAs[String](0).split("/")(0)).max

//  def lastParWithCategory(table: String, category: Int): String = spark.sql(s"show partitions $table")
//    .collect().map(_.getAs[String](0)).filter(_.endsWith(s"category=$category")).map(_.split("/")(0)).max

  override def run(): Unit = {
    spark.udf.register("weighted_2_vector", weighted2Vec(_: Double, _: Double))
    spark.udf.register("weighted_3_vector", weighted3Vec(_: Double, _: Double, _: Double))

    val results: ArrayBuffer[TopNSimilarity] = ArrayBuffer[TopNSimilarity]()

    featureViewWith(Apppkg2VecType.APP2VEC.id) match {
      case Some(app2vecView) =>
        results += oneDimesionTopResult(app2vecView, Apppkg2VecType.APP2VEC.id)
        (featureViewWith(Apppkg2VecType.ICON2VEC.id), featureViewWith(Apppkg2VecType.DETAIL2VEC.id)) match {
          case (Some(icon2vecView), Some(detail2vecView)) => // 1. 三个表都查到了数据
            results += oneDimesionTopResult(icon2vecView, Apppkg2VecType.ICON2VEC.id)
            results += oneDimesionTopResult(detail2vecView, Apppkg2VecType.DETAIL2VEC.id)
            // icon2view和detail2vecView均<100M, mapJoin
            val icon2vecViewMappingUDF = withSimilarityMappingUDF(icon2vecView)
            val detail2vecViewMappingUDF = withSimilarityMappingUDF(detail2vecView)
            sql(
              s"""
                 |select    app_name,
                 |          weighted_3_vector(similarity_value, $icon2vecViewMappingUDF(similarity_value),
                 |                            $detail2vecViewMappingUDF(similarity_value)) similarity_value,
                 |          apppkg, icon_path
                 |from      $app2vecView
                   """.stripMargin).createOrReplaceTempView("app_icon_detail_similarity_value_temp_view")
            // 3个都存在的时候计算综合排名
            results += oneDimesionTopResult("app_icon_detail_similarity_value_temp_view",
              Apppkg2VecType.APP2VEC.id ^ Apppkg2VecType.ICON2VEC.id ^ Apppkg2VecType.DETAIL2VEC.id)
          case (Some(icon2vecView), None) => // 2. 仅查到app&icon
            results += oneDimesionTopResult(icon2vecView, Apppkg2VecType.ICON2VEC.id)
            results += twoDimesionTopResult(app2vecView, icon2vecView,
              Apppkg2VecType.APP2VEC.id ^ Apppkg2VecType.ICON2VEC.id)
          case (None, Some(detail2vecView)) => // 3. 仅查到app&detail
            results += oneDimesionTopResult(detail2vecView, Apppkg2VecType.DETAIL2VEC.id)
            results += twoDimesionTopResult(app2vecView, detail2vecView,
              Apppkg2VecType.APP2VEC.id ^ Apppkg2VecType.DETAIL2VEC.id)
          case (None, None) => // 4. 仅查到app, do nothing
        }
      case None =>
    }

    val similarityResult = SimilarityResult(new String(Base64.encodeBase64(appName.getBytes())), results)

    println(similarityResult.toJsonString)

    if (jobContext.jobCommon.hasRPC()) {
      RpcClient.send(jobContext.jobCommon.rpcHost, jobContext.jobCommon.rpcPort,
        s"2\u0001${ jobContext.params.head.output.uuid }\u0002${ similarityResult.toJsonString }")
    }
  }

  def oneDimesionTopResult(viewName: String, category: Int): TopNSimilarity =
    TopNSimilarity(category,
      sql(s"""
             |--python中回调的中文可能出现异常, encode之后交给后端处理
             |select base64(app_name), similarity_value, apppkg, base64(icon_path) as icon_path
             |from (
             |  select app_name, similarity_value, apppkg, icon_path,
             |    row_number() over(order by similarity_value desc) rk
             |  from $viewName
             |) t
             |where rk <= $limit
           """.stripMargin).collect().map(r => AppInfo(r.getString(0),
        r.getAs[String]("icon_path"), r.getAs[String]("apppkg"), r.getDouble(1))))

  def twoDimesionTopResult(viewName1: String, viewName2: String, category: Int): TopNSimilarity = {
    val mappingUDF = withSimilarityMappingUDF(viewName2)
    sql(
      s"""
         |select    app_name,
         |          weighted_2_vector(similarity_value, $mappingUDF(similarity_value)) similarity_value,
         |          apppkg, icon_path
         |from      $viewName1
       """.stripMargin
    ).createOrReplaceTempView(s"two_dimesion_similarity_value_$category")
    oneDimesionTopResult(s"two_dimesion_similarity_value_$category", category)
  }


  def withSimilarityMappingUDF(viewName: String): String = {
    val udfName = s"similarity_mapping_$viewName"
    val mappingBC = sc.broadcast(spark.table(viewName).collect().map(r => r.getString(0) -> r.getDouble(1)).toMap)
    spark.udf.register(udfName, udf((appName: String) => mappingBC.value.getOrElse(appName, 0.0)))
    udfName
  }


  /**
   * 根据包名查出对应的vector, 并计算该包名和其他包的余弦乘积, 都是和同类做余弦乘积
   * 通过category标识vector的类型
   * category:
   *   1: app
   *   2: icon
   *   3: detail
   */
  def featureViewWith(category: Int): Option[String] = {
    val vectorMappingDay = getLastPar(HIVE_TABLE_APPPKG_VECTOR_MAPPING,
      0, s => s.split("/")(1).equals(s"category=$category")).split("=")(1)

    val dataArr = spark.table(HIVE_TABLE_APPPKG_VECTOR_MAPPING)
      .where(s"app_name='$appName' and category=$category and day=$vectorMappingDay")
      .select("features")
      .collect()
    if (dataArr.length == 0) {
      None
    } else {
      val viewName = s"cosine_similarity_${category}_temp_view"
      val data = dataArr.map(r => r.getSeq[Double](0)).head
      spark.udf.register(s"cosine_similarity_$category", udf((features: Seq[Double]) =>
                                                              cosineSimilarity(data, features)))
      val _res = sql(
        s"""
           |select app_name, if (app_name='$appName', 0, cosine_similarity_$category(features)) similarity_value,
           |  apppkg, icon_path
           |from $HIVE_TABLE_APPPKG_VECTOR_MAPPING
           |where day = $vectorMappingDay
           |  and category=$category and apppkg is not null and length(trim(apppkg)) > 0
         """.stripMargin)
      cacheImmediately(_res, viewName)
      Some(viewName)
    }
  }

  // 两向量余弦相似度计算
  def cosineSimilarity(f1: Seq[Double], f2: Seq[Double]): Double = {
    assert(f1.length == f2.length,
      s"vector size mismatch when cal cosine similarity, f1[${f1.length}], f2[${f2.length}]")

    val (_f1, _f2, _f3) = f1.zip(f2).foldLeft((0.0, 0.0, 0.0)) { case (prev, (left, right)) =>
      (prev._1 + left * right, prev._2 + math.pow(left, 2), prev._3 + math.pow(right, 2))
    }

    _f1 / (math.sqrt(_f2) * math.sqrt(_f3))
  }

  def tryOrZero(f: => Double): Double = {
    Try(f).getOrElse(0.0)
  }

  // 两向量加权: 2/((1/x) + (1/y))
  def weighted2Vec(v1: Double, v2: Double): Double = {
    if (v2 == 0.0) {
      v1
    } else if (v1 != 0.0) {
      tryOrZero(2 / (tryOrZero(1 / v1) + tryOrZero(1 / v2)))
    } else v2
  }

  // 三向量加权: 3/((1/x) + (1/y) + (1/z))
  def weighted3Vec(v1: Double, v2: Double, v3: Double): Double = {
    if (v2 == 0.0) {
      weighted2Vec(v1, v3)
    } else if (v3 == 0.0) {
      weighted2Vec(v1, v2)
    } else if (v1 != 0.0) {
      tryOrZero(3 / (tryOrZero(1 / v1) + tryOrZero(1 / v2) + tryOrZero(1 / v3)))
    } else weighted2Vec(v2, v3)
  }
}

case class SimilarityResult(appName: String, topN: Seq[TopNSimilarity]) {
  def toJsonString: String = {
    val json =
      ("appName" -> appName) ~ ("topn" -> seq2jvalue(topN.map(_.toJson)))
    compact(render(json))
  }
}
case class TopNSimilarity(category: Int, values: Seq[AppInfo]) {
  def toJson: JValue = {
    ("category" -> category) ~ ("values" -> seq2jvalue(values.map(_.toJvalue())))
  }
}

case class AppInfo(appName: String, icon: String, apppkg: String, score: Double) {
  def toJvalue(): JValue = {
    import org.json4s.{ Extraction, NoTypeHints }
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)

    Extraction.decompose(this)
  }
}