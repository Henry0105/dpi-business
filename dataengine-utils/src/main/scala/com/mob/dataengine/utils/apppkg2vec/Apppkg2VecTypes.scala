package com.mob.dataengine.utils.apppkg2vec

import com.mob.dataengine.commons.enums.Apppkg2VecType._
import com.mob.dataengine.commons.traits.Cacheable
import com.mob.dataengine.commons.utils.PropUtils.{HIVE_TABLE_APPPKG_TMP_APP2VEC_MAPPING, _}
import com.mob.dataengine.utils.apppkg2vec.App2VecType.srcTable
import com.mob.dataengine.utils.apppkg2vec.Apppkg2VectorPreparation.Params
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Try

abstract class Apppkg2VecTypes(val category: Int,
                               val value: String) extends Cacheable {

  val srcTable: String

  def run(params: Params): DataFrame

  def lastPar(table: String): String = spark.sql(s"show partitions $table"
                                           ).collect().map(_.getAs[String](0).split("/")(0)).max
}

case object App2VecType extends Apppkg2VecTypes(APP2VEC.id, APP2VEC.toString) {

  lazy override implicit val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  override val srcTable = HIVE_TABLE_APPPKG_APP2VEC_PAR_WI
  val numDimesions = 100
  import spark.implicits._
  import spark. {sparkContext => sc}

  /**
   * 根据游戏类别对app2vec源表数据过滤, 将apppkg映射为appname, 同时过滤, 仅保留游戏行业
   */
  override def run(params: Params): DataFrame = {

    val featureCols = (1 to numDimesions).map(d => s"d$d")
    val _lastPar = lastPar(srcTable)

    println(s"cal app2vec with table[$srcTable], par[${_lastPar}]")

    // TODO 由于app2vec目前采用cnt>=3的计算逻辑, apppkg数量过大 ≈ 100W, 所以进行cnt限制
    // 这张表不稳妥, 不严格依赖, 如果有, 就使用, 如果是空表或者表被删了, 就不用
    val cntFilterOptionSql = Try {
      val apppkgWithCntAboveThreshold = sc.broadcast(spark.table(HIVE_TABLE_APPPKG_TMP_APP2VEC_MAPPING
                                                         ).where(lastPar(HIVE_TABLE_APPPKG_TMP_APP2VEC_MAPPING)
                                                         ).where(s"cnt>=${params.cnt}"
                                                         ).select("pkg").collect().map(_.getString(0)).toSet)
      if (apppkgWithCntAboveThreshold.value.nonEmpty) {
        spark.udf.register("valid_apppkg", udf((apppkg: String) => apppkgWithCntAboveThreshold.value.contains(apppkg)))
        println(s"table[$HIVE_TABLE_APPPKG_TMP_APP2VEC_MAPPING] exists," +
          s"use filter with apppkg cnt[${apppkgWithCntAboveThreshold.value.size}]")
        "and      valid_apppkg(apppkg)"
      } else ""
    }.getOrElse("")

    // TODO apppkg2name 可能需要sortMergeJoin, 维表较大
    val _res = sql(
      s"""
         |select   app_name, vector_comb_by_avg(features) features
         |from  (
         |         select   app_name, features
         |         from  (
         |                  select   apppkg,
         |                           array(${featureCols.mkString(",")}) features
         |                  from     $srcTable
         |                  where    ${_lastPar} and is_game_apppkg(apppkg)
         |                  $cntFilterOptionSql
         |         ) t1
         |         join     $HIVE_TABLE_RP_APP_NAME_INFO t2
         |         on       t1.apppkg = t2.apppkg
         |) t
         |group by app_name
       """.stripMargin).cache()

    // 全排序 ↓↓↓
    val parCounts: Seq[(Int, Int)] = _res.rdd.mapPartitionsWithIndex{ case (idx, ite) =>
      Map(idx -> ite.length).iterator
    }.collect().toSeq

    println(s"totalCount: ${parCounts.map(_._2).sum}")
    println(s"parCounts: $parCounts")

    val aggParCnt = spark.sparkContext.broadcast(parCounts.sortBy(_._1).scanLeft(0 -> 0) { case (prev, (idx, cnt)) =>
                                                                val (_, prevCnt) = prev
                                                                (idx + 1) -> (prevCnt + cnt)
                                                            }.dropRight(1).toMap)

    _res.select("app_name").rdd.mapPartitionsWithIndex{ case (idx, ite) =>
      var start = aggParCnt.value(idx)
      ite.map { case Row(appName: String) =>
        val _res = (appName, start)
        start += 1
        _res
      }
    }.toDF("app_name", "index").createOrReplaceTempView("app_name_index_temp_view")
    // 全排序 ↑↑↑

    //_res.select("app_name").map(_.getString(0)).collect().toSeq.zipWithIndex.toDF("app_name", "index"
    //   ).createOrReplaceTempView("app_name_index_temp_view")

    // 将app2vec中筛选出的game进行索引, 方便后续计算
    sql(
      s"""
         |insert overwrite table $HIVE_TABLE_APPPKG_INDEX_MAPPING_PAR partition(day='${Apppkg2VectorPreparation.day}')
         |select   app_name, index
         |from     app_name_index_temp_view
       """.stripMargin)

    _res
  }
}

/**
 * 计算后仅保留x表中的app_name
 */
case object Icon2VecType extends Apppkg2VecTypes(ICON2VEC.id, ICON2VEC.toString) {

  lazy override implicit val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  override val srcTable = HIVE_TABLE_APPPKG_ICON2VEC_PAR_WI
  import spark.implicits._
  val _lastPar = lastPar(srcTable)

  /**
   * 根据预处理的结果, 合并相同游戏名称的特征
   */
  override def run(params: Params): DataFrame = {

    println(s"cal icon2vec with table[$srcTable], par[${_lastPar}]")

    sql(
      s"""
         |select   t1.app_name, features
         |from  (
         |         select   app_name, vector_comb_by_avg(features) features
         |         from  (
         |                  select    app_name, to_double_array(split(features, ',')) features
         |                  from      $srcTable
         |                  where     ${_lastPar}
         |         ) t
         |         group by app_name
         |) t1
         |join  ( -- join的目的是为了保留x表中的app_name
         |         select   app_name
         |         from     $HIVE_TABLE_APPPKG_INDEX_MAPPING_PAR
         |         where    day='${Apppkg2VectorPreparation.day}'
         |) t2  on t1.app_name = t2.app_name
       """.stripMargin)
  }
}

/**
 * 计算后仅保留x表中的app_name
 */
case object Detail2VecType extends Apppkg2VecTypes(DETAIL2VEC.id, DETAIL2VEC.toString) {

  lazy override implicit val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  override val srcTable = HIVE_TABLE_APPPKG_ICON2VEC_PAR_WI
  import spark.implicits._
  val tapTapLastPar = lastPar(HIVE_TABLE_MOBSPIDER_TAPTAP_DETAIL)
  val diyiyouLastPar = lastPar(HIVE_TABLE_DIYIYOU_GAME_DETAIL)

  override def run(params: Params): DataFrame = {

    println(s"cal detail2vec with" +
      s"table[$HIVE_TABLE_APP_DETAILS_SDK, par_day>=${Apppkg2VectorPreparation.day180Before}]," +
      s"table[$HIVE_TABLE_MOBSPIDER_TAPTAP_DETAIL, day=$tapTapLastPar]," +
      s"table[$HIVE_TABLE_DIYIYOU_GAME_DETAIL, day=$diyiyouLastPar]")

    val _res = sql(
      s"""
         |select   m1.app_name, tag
         |from  (
         |         select   app_name, parse_words(concat_ws('', collect_list(desc))) tag
         |         from  (
         |                  select   name app_name, description desc
         |                  from  (
         |                           select   name, description, par_day,
         |                                    row_number() over(partition by name order by par_day desc) rk
         |                           from     $HIVE_TABLE_APP_DETAILS_SDK
         |                           where    (is_game_appname(name) or description rlike '[手游|游戏]')
         |                           and      par_day >= '${Apppkg2VectorPreparation.day180Before}'
         |                           and      valid_desc(description)
         |                  ) t1
         |                  where    rk <= 1
         |
         |                  union all
         |
         |                  select   app_name, describe desc
         |                  from     $HIVE_TABLE_MOBSPIDER_TAPTAP_DETAIL
         |                  where    $tapTapLastPar
         |                  and      valid_desc(describe)
         |
         |                  union all
         |
         |                  select   game_name app_name, intro desc
         |                  from     $HIVE_TABLE_DIYIYOU_GAME_DETAIL
         |                  where    $diyiyouLastPar
         |                  and      valid_desc(intro)
         |         ) m
         |         group by app_name
         |) m1
         |join  (
         |         select   app_name
         |         from     $HIVE_TABLE_APPPKG_INDEX_MAPPING_PAR
         |         where    day='${ Apppkg2VectorPreparation.day }'
         |) m2  on m1.app_name = m2.app_name
       """.stripMargin
    )

    val word2vec = new Word2Vec().setInputCol("tag")
                                 .setOutputCol("features")
                                 .setVectorSize(100)
                                 .setMinCount(0)

    word2vec.fit(_res).transform(_res).select($"app_name", callUDF("vec2arr", $"features").as("features"))
  }
}
