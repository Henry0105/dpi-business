package com.mob.dataengine.utils.apppkg2vec

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.ansj.recognition.impl.FilterRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import com.mob.dataengine.commons.traits.{Cacheable, TableTrait}
import com.mob.dataengine.commons.utils.PropUtils._
import com.mob.dataengine.utils.DateUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scopt.OptionParser

import scala.io.Source
import scala.util.Try

/**
 * @author suny
 * 两部分计算逻辑
 * ① http://j.mob.com/browse/MPLUSTC-432中x,y,z表生成, 对应结果表apppkg_vector_data_opt的三个分区, 以category区分
 * 1. app2vec[category=1]:    根据app2vec, 筛选游戏行业的appname, 输出
 * 2. icon2vec[category=2]:   根据icon的增量特征数据表, 合并taptap和第一手游的features, 按照appname维度聚合
 * 3. detail2vec[category=4]: 根据appname的详情, 通过Word2Vec词向量化计算 appname2vec
 *
 * 数据量 =>
 * 源表:
 * rp_mobdi_app.apppkg_app2vec_par_wi:    600M|100W
 * dm_sdk_mapping.rp_app_name_info:       1.5G|1e
 * rp_mobdi_app.app_details_sdk:          1.0G|180W
 * dw_ext_crawl.mobspider_taptap_detail:   40M|77290
 * dw_ext_crawl.diyiyou_game_detail:      160M|137570
 * dm_sdk_mapping.app_category_mapping_par: 2M|41702
 * apppkg_icon2vec_par_wi: [source_type=taptap |source_type=dysy ]
 *
 * 结果表:
 * apppkg_vector_data_opt(cnt>=0 ):  [category=1 738112|category=2 19342|category=4 77610]
 * apppkg_vector_data_opt(cnt>=30):  [category=1 472518|category=2 16145|category=4 67174]
 * apppkg_vector_data_opt(cnt>=50):  [category=1 374380|category=2 14371|category=4 61935]
 * apppkg_vector_data_opt(cnt>=100): [category=1 269177|category=2 12149|category=4 54625]
 * apppkg_index_mapping_par: 472518
 *
 * 任务耗时: ≈ 10分钟(主要耗时在Word2Vec)
 */
object Apppkg2VectorPreparation extends Cacheable with TableTrait {

  case class Params(day: String = DateUtils.currentDay(),
                    cnt: Int = 50, // 对app2vec输出进行限制的阈值
                    rpcHost: Option[String] = None,
                    rpcPort: Option[Int] = None) extends Serializable {

    def isRpc: Boolean = rpcHost.isDefined && rpcPort.isDefined
  }


  @transient lazy override implicit val spark: SparkSession = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._
  import spark. { sparkContext => sc}
  var day: String = _
  lazy val day180Before = LocalDate.parse(day, DateTimeFormatter.ofPattern("yyyyMMdd")).plusDays(-180
                                  ).format(DateTimeFormatter.ofPattern("yyyyMMdd"))


  def main(args: Array[String]): Unit = {
    val defaultParams: Params = Params()
    val projectName = s"Apppkg2VectorPreparation_[${DateUtils.currentDay()}]"

    val parser = new OptionParser[Params](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .text("day 例如:20180806 默认当天")
        .action((x, c) => c.copy(day = x))
      opt[Int]('c', "cnt")
        .text("cnt, 例如: 50")
        .action((x, c) => c.copy(cnt = x))
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
        spark.stop()
      case _ => sys.exit(1)
    }
  }

  def runMain(params: Params): Unit = {

    implicit val day: String = params.day

    val nonGameApppkgs = getAppInfo("apppkg")
    val nonGameAppNames = getAppInfo("appname")
    // 根据表app_category_mapping_par中非"游戏服务"类别的apppkg去筛选
    spark.udf.register("is_game_apppkg", udf((apppkg: String) =>
      Try(!nonGameApppkgs.value.contains(apppkg)).getOrElse(false)))
    // 根据表app_category_mapping_par中非"游戏服务"类别的appname去筛选
    spark.udf.register("is_game_appname", udf((appName: String) =>
      Try(!nonGameAppNames.value.contains(appName)).getOrElse(false)))
    // 去除详情字段中特殊字符和字母数字之后, 长度小于20的需要被过滤掉
    spark.udf.register("valid_desc", udf((desc: String) => desc.replaceAll(
      "[\\n`~!@#$%^&*()+=|{}':;',\\\\[\\\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。， 、？\\w]", "").trim.length >= 20))
    // 根据表dm_sdk_mapping.rp_app_name_info映射apppkg为app_name
    // TODO 维表太大, 放弃mapJoin
    //spark.udf.register("apppkg2name", udf((apppkg: String) => apppkg2appname.value.getOrElse(apppkg, null)))
    // 将array<string>转化为array<double>
    spark.udf.register("to_double_array", udf((features: Seq[String]) =>
      features.map(f => Try(f.toDouble).getOrElse(Double.NaN))))
    // 将稠密的两个向量通过均值计算合并
    spark.udf.register("vector_comb_by_avg", VectorCombByAvgUDAF)
    // 通过ansj框架对应用详情进行分词操作
    spark.udf.register("parse_words", WordSplitterUDF.parse _)
    // 将SparseVector转化为array
    spark.udf.register("vec2arr", udf((features: org.apache.spark.ml.linalg.DenseVector) => features.toArray))

    Seq(App2VecType, Icon2VecType, Detail2VecType).foreach(v => sink(v.run(params), v.category, v.value))

    val metrics = sql(
      s"""
         |select   day, category, count(app_name)
         |from     $HIVE_TABLE_APPPKG_VECTOR_MAPPING
         |where    day=$day
         |group by day, category
       """.stripMargin).collect().map(r => s"${r.getString(0)}|${r.getInt(1)}|${r.getLong(2)}").mkString("|", "\n", "|")

    println(
      s"""
         |----------------------------------
         |Apppkg2VectorPreparation calculation success, metrics:
         |table[$HIVE_TABLE_APPPKG_VECTOR_MAPPING], partition[day=$day]
         |table[$HIVE_TABLE_APPPKG_INDEX_MAPPING_PAR], partition[day=$day]
         |
         ||day|catogory|count|
         |$metrics
         |----------------------------------
       """.stripMargin)
  }

  def sink(f: DataFrame, category: Int, value: String): Unit = {
    f.createOrReplaceTempView(s"${value}_temp_view")
    // taptap的图标目录:  /mount_data/logs/taptap/icon/${month},eg: 202004/xx.png
    val iconDay = getLastPar(HIVE_TABLE_APPPKG_ICON2VEC_PAR_WI).split("=")(1)

    sql(
      s"""
         |insert overwrite table $HIVE_TABLE_APPPKG_VECTOR_MAPPING
         |partition (day=$day, category=$category)
         |select a.app_name, apppkg,
         |  concat('/mount_data/logs/taptap/icon/', substring(c.day, 1, 6), image_name) icon_path, a.features
         |from ${value}_temp_view as a
         |left join $HIVE_TABLE_RP_APP_NAME_INFO as b
         |on a.app_name = b.app_name
         |left join $HIVE_TABLE_APPPKG_ICON2VEC_PAR_WI as c
         |on a.app_name = c.app_name
         |where c.day = $iconDay and c.source_type = 'taptap'
       """.stripMargin)
  }

  // ↓↓↓ 下面都是UDF用
  def getAppInfo(colName: String): Broadcast[Set[String]] =
    sc.broadcast(spark.table(HIVE_TABLE_APP_CATEGORY_MAPPING_PAR
                     ).where(s"version='1000' and cate_l1 <> '游戏服务' and $colName is not null"
                     ).select(colName
                     ).collect(
                     ).map(_.getString(0)).toSet)

  //val apppkg2appname = sc.broadcast(spark.table(HIVE_TABLE_RP_APP_NAME_INFO
  //                                           ).select("apppkg", "app_name"
  //                                           ).map(r => r.getString(0) -> r.getString(1)
  //                                           ).collect().toMap)

}
