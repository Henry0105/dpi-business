package com.mob.dataengine.utils.apppkg2vec

import com.mob.dataengine.commons.traits.Cacheable
import com.mob.dataengine.utils.DateUtils
import com.mob.dataengine.commons.utils.PropUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

import scala.util.Try

/**
 * 根据表apppkg_vector_data_opt计算相似度及加权, 共6种结果
 * 1. [category=1]app2vec
 * 2. [category=2]icon2vec
 * 3. [category=4]detail2vec
 * 4. [category=3]app2vec & icon2vec
 * 5. [category=5]app2vec & detail2vec
 * 6. [category=7]app2vec & icon2vec & detail2vec
 *
 * 100W维的两个Seq[Double], 计算余弦相似度的耗时约为: 600ms, 单行约167小时[1000000*600ms]
 * 10W维的两个Seq[Double], 计算余弦相似度的耗时约为: 40ms, 单行约[1小时]
 *
 * 任务耗时: app2vecCnt=269177, 1h
 */
object ApppkgTopSimilarity extends Cacheable {

  case class Params(day: String = DateUtils.currentDay(),
    rpcHost: Option[String] = None,
    rpcPort: Option[Int] = None) extends Serializable {

    def isRpc: Boolean = rpcHost.isDefined && rpcPort.isDefined
  }

  @transient lazy override implicit val spark = SparkSession.builder()
                                                            .appName(this.getClass.getSimpleName)
                                                            .enableHiveSupport()
                                                            .getOrCreate()
  import spark.implicits._
  import spark.{ sparkContext => sc }
  var day: String = _

  def main(args: Array[String]): Unit = {

    val defaultParams: Params = Params()
    val projectName = s"Apppkg2VectorPreparation_[${DateUtils.currentDay()}]"

    val parser = new OptionParser[Params](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .text("day 例如:20180806 默认当天")
        .action((x, c) => c.copy(day = x))
      opt[Int]('p', "rpcPort")
        .text(s"thrift rpc port")
        .action((x, c) => c.copy(rpcPort = Some(x)))
      opt[String]('h', "rpcHost")
        .text(s"thrift rpc host")
        .action((x, c) => c.copy(rpcHost = Some(x)))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        day = params.day
        println(params)
        runMain(params)
      case _ => sys.exit(1)
    }

    spark.stop()
  }

  def runMain(params: Params): Unit = {

    // appname -> index映射表
    val a2i: Broadcast[Map[String, Int]] = sc.broadcast(spark.table(HIVE_TABLE_APPPKG_INDEX_MAPPING_PAR
                                                            ).where(lastPar(HIVE_TABLE_APPPKG_INDEX_MAPPING_PAR)
                                                            ).select("app_name", "index"
                                                            ).collect(
                                                            ).map(r => r.getString(0) -> r.getInt(1)
                                                            ).toMap)
    // index -> appname映射表
    val i2a: Broadcast[Map[Int, String]] = sc.broadcast(a2i.value.map { case (k, v) => v -> k})

    spark.udf.register("similarity_top30", sortAndLimit(_: SparseVector, 30, i2a))
    spark.udf.register("weighted_2_vector", weighted2Vec(_: SparseVector, _: SparseVector))
    spark.udf.register("weighted_3_vector", weighted3Vec(_: SparseVector, _: SparseVector, _: SparseVector))

    // 计算x,y,z每个表每个app和其它所有app相似度
    cacheImmediately(similarityCalWithVecType(App2VecType, a2i), "app2vec_cosine_similarity_temp_view")
    cacheImmediately(similarityCalWithVecType(Icon2VecType, a2i), "icon2vec_cosine_similarity_temp_view")
    cacheImmediately(similarityCalWithVecType(Detail2VecType, a2i), "detail2vec_cosine_similarity_temp_view")

    // 计算加权结果, 排序并输出
    // ① category=1
    sink(sql("select app_name, similarity_top30(similarity_value) top from app2vec_cosine_similarity_temp_view"),
         App2VecType.category)
    sink(sql("select app_name, similarity_top30(similarity_value) top from icon2vec_cosine_similarity_temp_view"),
         Icon2VecType.category)
    sink(sql("select app_name, similarity_top30(similarity_value) top from detail2vec_cosine_similarity_temp_view"),
         Detail2VecType.category)
    sink(sql(
      s"""
         |select   t1.app_name, similarity_top30(weighted_2_vector(t1.similarity_value, t2.similarity_value)) top
         |from     app2vec_cosine_similarity_temp_view t1
         |join     icon2vec_cosine_similarity_temp_view t2
         |on       t1.app_name = t2.app_name
       """.stripMargin),
         App2VecType.category ^ Icon2VecType.category)
    sink(sql(
      s"""
         |select   t1.app_name, similarity_top30(weighted_2_vector(t1.similarity_value, t2.similarity_value)) top
         |from     app2vec_cosine_similarity_temp_view t1
         |join     detail2vec_cosine_similarity_temp_view t2
         |on       t1.app_name = t2.app_name
       """.stripMargin),
         App2VecType.category ^ Detail2VecType.category)
    sink(sql(
      s"""
         |select   t1.app_name,
         |        similarity_top30(weighted_3_vector(t1.similarity_value, t2.similarity_value, t3.similarity_value)) top
         |from     app2vec_cosine_similarity_temp_view t1
         |join     icon2vec_cosine_similarity_temp_view t2
         |join     detail2vec_cosine_similarity_temp_view t3
         |on       t1.app_name = t2.app_name and t1.app_name = t3.app_name
       """.stripMargin),
         App2VecType.category ^ Icon2VecType.category ^ Detail2VecType.category)

    val metrics = sql(
      s"""
         |select   day, category, count(app_name)
         |from     $HIVE_TABLE_APPPKG_GAME_COMPETION_DATA_OPT
         |where    day=$day
         |group by day, category
       """.stripMargin).map(r => s"|${r.getString(0)}|${r.getInt(1)}|${r.getLong(2)}|").collect().mkString("\n")

    println(
      s"""
         |----------------------------------
         |ApppkgTopSimilarity calculation success, metrics:
         |table[$HIVE_TABLE_APPPKG_VECTOR_MAPPING], partition[day=$day]
         |
         ||day|category|count|
         |$metrics
         |----------------------------------
       """.stripMargin)
  }

  def sink(f: => DataFrame, category: Int): Unit = {
    f.createOrReplaceTempView(s"weighted_vector_category${category}_temp_view")
    sql(
      s"""
         |insert overwrite table $HIVE_TABLE_APPPKG_GAME_COMPETION_DATA_OPT
         |partition (day=$day, category=$category)
         |select   app_name, top
         |from     weighted_vector_category${category}_temp_view
       """.stripMargin)
  }

  /**
   * 单维度相似度计算
   * @param vecType app2vec|icon2vec|detail2vec
   * @return 笛卡尔积余弦相似度计算
   */
  def similarityCalWithVecType(vecType: Apppkg2VecTypes, a2i: Broadcast[Map[String, Int]]): DataFrame = {

    // 加载x,y,z三张表的数据同时广播, 计算余弦相似度
    // TODO 广播的计算逻辑尽量不要更改, 由于需要计算笛卡尔积, 可以考虑适当增大内存
    def vectorWith(vecType: Apppkg2VecTypes): (String, Broadcast[Array[(String, Seq[Double])]]) = {
      val viewName = s"vector_src_data_temp_view_${vecType.category}"
      val _df = spark.table(HIVE_TABLE_APPPKG_VECTOR_MAPPING).where(lastPar(HIVE_TABLE_APPPKG_VECTOR_MAPPING)
                                                             ).where(s"category=${vecType.category}"
                                                             ).select("app_name", "features").repartition(1000).cache()
      _df.createOrReplaceTempView(viewName)
      viewName -> sc.broadcast(_df.collect().map(r => r.getString(0) -> r.getSeq[Double](1)))
    }

    val (vecView, vecBC) = vectorWith(vecType)
    spark.udf.register(s"cosine_similarity_${vecType.category}",
      cosineSimilarity(_: String, _: Seq[Double], vecBC, a2i))

    sql(
      s"""
         |select   app_name, cosine_similarity_${vecType.category}(app_name, features) similarity_value
         |from     $vecView
       """.stripMargin)
  }


  def lastPar(table: String): String = spark.sql(s"show partitions $table"
                                           ).collect().map(_.getAs[String](0).split("/")(0)).max


  def cosineSimilarity(appName: String,
                       input: Seq[Double],
                       vecBC: Broadcast[Array[(String, Seq[Double])]],
                       a2i: Broadcast[Map[String, Int]]): SparseVector = {

    val eles = vecBC.value.map { case (_appName, _features) =>
      if (appName.equalsIgnoreCase(_appName)) {
        a2i.value(_appName) -> 0.0
      } else a2i.value(_appName) -> cosineSimilarityBetweenVector(_features, input)
    }//.sortBy(_._1).map(_._2)
    Vectors.sparse(a2i.value.size, eles).toSparse
  }

  // 两向量余弦相似度计算
  def cosineSimilarityBetweenVector(f1: Seq[Double], f2: Seq[Double]): Double = {
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

  def tryOrZeroNew(eles: Double*): Double = {
    val _res = eles.filter(_ != 0.0)
    if (_res.isEmpty) {
      0.0
    } else {
      _res.length / _res.map(1 / _).sum
    }
  }

  // 两向量加权: 2/((1/x) + (1/y))
  def weighted2Vec(v1: SparseVector, v2: SparseVector): SparseVector = {
    if (v2 == null) {
      v1
    } else if (v1 != null) {
      // 在这个计算里, v1是个稠密矩阵, 遍历v2快速计算
      for (idx <- v2.indices.indices) {
        val index = v2.indices(idx)
        v1.values(index) = tryOrZeroNew(v1.values(index) + v2.values(idx))
      }
      v1
    } else {
      v2
    }
    //val values = v1.toDense.toArray.zip(v2.toDense.toArray).map { case (e1, e2) =>
    //              tryOrZero(2 / (tryOrZero(1 / e1) + tryOrZero(1 / e2))) }
    //Vectors.sparse(v1.size, v1.indices, values).toSparse
    // 入参是Seq[Double]时使用
    //if (v1.isEmpty) {
    //  v2
    //} else {
    //  v1.zip(v2).map { case (e1, e2) => tryOrZero(2 / (tryOrZero(1 / e1) + tryOrZero(1 / e2))) }
    //}

  }

  // 三向量加权: 3/((1/x) + (1/y) + (1/z))
  def weighted3Vec(v1: SparseVector, v2: SparseVector, v3: SparseVector): SparseVector = {
    if (v2 == null) {
      weighted2Vec(v1, v3)
    } else if (v3 == null) {
      weighted2Vec(v1, v2)
    } else if (v1 != null) {
      var i2 = 0
      var i3 = 0
      while (i2 < v2.indices.length && i3 < v3.indices.length) {
        val idx2 = v2.indices(i2)
        val idx3 = v3.indices(i3)
        if (idx2 < idx3) {
          v1.values(idx2) = tryOrZeroNew(v1.values(idx2), v2.values(i2))
          i2 += 1
        } else if (idx2 > idx3) {
          v1.values(idx3) = tryOrZeroNew(v1.values(idx3), v3.values(i3))
          i3 += 1
        } else {
          v1.values(idx2) = tryOrZeroNew(v1.values(idx2), v2.values(i2), v3.values(i3))
          i2 += 1
          i3 += 1
        }
      }
      // 处理剩余的values
      while (i3 < v3.indices.length) {
        val idx3 = v3.indices(i3)
        v1.values(idx3) = tryOrZero(2 / (tryOrZero(1 / v1.values(idx3)) + tryOrZero(1 / v3.values(idx3))))
        i3 += 1
      }
      while (i2 < v2.indices.length) {
        val idx2 = v2.indices(i2)
        v1.values(idx2) = tryOrZero(2 / (tryOrZero(1 / v1.values(idx2)) + tryOrZero(1 / v2.values(idx2))))
        i2 += 1
      }
      v1
    } else {
      weighted2Vec(v2, v3)
    }
  }

  // 按相似度计算结果倒排序, 映射appName取topN
  def sortAndLimit(vector: SparseVector, limit: Int, i2a: Broadcast[Map[Int, String]]): Seq[String] = {
    // 停用
    //vector.indices.zip(vector.values).sortBy(_._2)(Ordering.Double.reverse
    //                                ).map(t => s"${i2a.value(t._1)}\u0001${t._2}").take(limit)

    // 换个topK方法, 用最小堆, 缓解内存压力
    implicit object TupleOrd extends math.Ordering[(Int, Double)] {
      override def compare(x: (Int, Double), y: (Int, Double)) =
        if (x._2 > y._2) 1 else if (x._2 < y._2) -1 else 0
    }
    val queue = new BoundedPriorityQueue[(Int, Double)](30)
    var idx = 0
    while (idx < vector.indices.length) {
      queue += (vector.indices(idx) -> vector.values(idx))
      idx += 1
    }
    queue.toSeq.sortBy(_._2)(Ordering.Double.reverse).map(t => s"${i2a.value(t._1)}\u0001${t._2}")
    // 按照Seq[Double]为入参的方法
    //vector.zipWithIndex.map { case (value, idx) =>
    //  value -> i2a.value(idx)
    //}.sortBy(_._1)(Ordering.Double.reverse).map(_._2).take(limit)
  }
}
