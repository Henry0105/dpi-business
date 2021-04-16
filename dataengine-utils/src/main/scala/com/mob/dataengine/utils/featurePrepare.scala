package com.mob.dataengine.utils

import org.apache.spark.ml.linalg.{DenseVector, Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vectors => mllibVectors}
import org.apache.spark.ml.feature.{IndexToString, StandardScaler, StringIndexer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{max, min}
import com.mob.dataengine.commons.utils.{PropUtils => PU}
import com.mob.dataengine.utils.tags.profile.TagsGeneratorHelper.sql
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import Array._

case class MaxMin(maxV: Double, minV: Double, DiffV: Double)


object featurePrepare {

  lazy private[this] val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val projectName = s"Lookalike[${DateUtils.currentDay()}]"

    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName(projectName)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val preDate = "20190101"

    val profileDF = sql(spark,
      s"""
         |SELECT device, applist, cell_factory, province_cn, city_level, tag_list
         |FROM ${PU.HIVE_TABLE_RP_DEVICE_PROFILE_FULL}
         |WHERE processtime >= '$preDate'
         |AND SIZE(SPLIT(SPLIT(tag_list,'=')[0], ',')) >= 3
      """.stripMargin)

    profileDF.createOrReplaceTempView("profile_df")


    val cateIDMaxMin = sql(spark,
      s"""
         |SELECT MAX(cate_l1_id) AS max_index,
         |MIN(cate_l1_id) AS min_index
         |FROM ${PU.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR}
       """.stripMargin)
      .map(row => MaxMin(
        row.getAs[String]("max_index").toDouble,
        row.getAs[String]("min_index").toDouble,
        row.getAs[String]("max_index").toDouble - row.getAs[String]("min_index").toDouble
      )
      )
      .head
    val appCateDF = sql(spark,
      s"""
         |SELECT profile_tbl.device
         |        ,COALESCE(appcat_tbl.cate_l1_id, ${cateIDMaxMin.maxV + 1}) AS cate_l1_id
         |        ,COUNT(1) AS cat_cnt
         |FROM (
         |        SELECT apppkg, MAX(cate_l1_id) AS cate_l1_id
         |        FROM ${PU.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR}
         |        WHERE version = '1000'
         |        GROUP BY apppkg
         |) appcat_tbl
         |RIGHT JOIN (
         |        SELECT device, app
         |        FROM profile_df
         |        LATERAL VIEW EXPLODE(SPLIT(applist, ',')) s as app
         |) profile_tbl ON appcat_tbl.apppkg = profile_tbl.app
         |GROUP BY profile_tbl.device, COALESCE(appcat_tbl.cate_l1_id, ${cateIDMaxMin.maxV + 1})
      """.stripMargin)

    appCateDF.createOrReplaceTempView("app_cate_df")

    val startDate = "20190106"
    val endDate = "20190303"
    val featureMaxMin = sql(spark,
      s"""
         |SELECT MAX(split(feature, '_')[0]) AS max_index,
         |MIN(split(feature, '_')[0]) AS min_index
         |FROM ${PU.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}
         |WHERE timewindow = '7'
         |     AND flag IN (0, 1)
         |     AND day >= '$startDate'
         |     AND day <= '$endDate'
         |     AND LENGTH(feature) = 8
       """.stripMargin).cache()
      .map(row => MaxMin(
        row.getAs[String]("max_index").toDouble,
        row.getAs[String]("min_index").toDouble,
        row.getAs[String]("max_index").toDouble - row.getAs[String]("min_index").toDouble)
      )
      .head
    val installAndUninstallDF = sql(spark,
      s"""
         |SELECT p_tbl.device, online_tbl.feature, online_tbl.cnt
         |FROM profile_df p_tbl
         |JOIN (
         |        SELECT  device
         |                ,COALESCE(feature,
         |                    CONCAT_WS('_',
         |                        CAST(CAST(${featureMaxMin.maxV} AS int) + 1 AS string),
         |                        CAST(flag AS string), '_7')
         |                    ) AS feature
         |                ,SUM(cnt) AS cnt
         |        FROM (
         |            SELECT device, feature, cnt, flag
         |            FROM ${PU.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}
         |            WHERE timewindow = '7'
         |                AND flag IN (0, 1)
         |               AND day >= '$startDate'
         |               AND day <= '$endDate'
         |               AND LENGTH(feature) = 8
         |        ) limit_tbl
         |        GROUP BY device, feature, flag
         |) online_tbl
         |ON p_tbl.device = online_tbl.device
       """.stripMargin
    )

    installAndUninstallDF.createOrReplaceTempView("in_un_df")


    val featureEncodeDF = sql(spark,
      s"""
         |SELECT type, row_number() over(order by type ) AS index
         |FROM (
         |        SELECT cate_l1_id AS type
         |        FROM app_cate_df
         |        GROUP BY cate_l1_id
         |
         |        UNION ALL
         |        SELECT feature AS type
         |        FROM in_un_df
         |        GROUP BY feature
         |) app_cate_inunstall
       """.stripMargin
    ).cache()

    featureEncodeDF.createOrReplaceTempView("fe_df")

    val feIndexMaxMin = featureEncodeDF
      .agg(
        max("index").alias("max_index"),
        min("index").alias("min_index")
      ).map(row => MaxMin(
        row.getAs[Int]("max_index").toDouble,
        row.getAs[Int]("min_index").toDouble,
        row.getAs[Int]("max_index").toDouble - row.getAs[Int]("min_index").toDouble
      ))
      .head


    val cfDF = profileDF.select("device", "cell_factory")
    val cfIndexer = new StringIndexer()
      .setInputCol("cell_factory")
      .setOutputCol("cfIndex")
      .fit(cfDF)
    val cfIndexedDF = cfIndexer.transform(cfDF)

    // val cfInputColSchema = cfIndexedDF.schema(cfIndexer.getOutputCol)

    val cfConverter = new IndexToString()
      .setInputCol("cfIndex")
      .setOutputCol("originalCF")

    val cfConvertedDF = cfConverter.transform(cfIndexedDF)
    cfConvertedDF.createOrReplaceTempView("cf_conv_df")


    val pcnDF = profileDF.select("device", "province_cn")
    val pcnIndexer = new StringIndexer()
      .setInputCol("province_cn")
      .setOutputCol("pcnIndex")
      .fit(pcnDF)
    val pcnIndexedDF = pcnIndexer.transform(pcnDF)

    // val pcnInputColSchema = pcnIndexedDF.schema(pcnIndexer.getOutputCol)

    val pcnConverter = new IndexToString()
      .setInputCol("pcnIndex")
      .setOutputCol("originalPCN")

    val pcnConvertedDF = pcnConverter.transform(pcnIndexedDF)
    pcnConvertedDF.createOrReplaceTempView("pcn_conv_df")

    val cityLevelDF = profileDF.select("device", "city_level")
    val clIndexer = new StringIndexer()
      .setInputCol("city_level")
      .setOutputCol("clIndex")
      .fit(cityLevelDF)
    val clIndexedDF = clIndexer.transform(cityLevelDF)

    // val clInputColSchema = clIndexedDF.schema(clIndexer.getOutputCol)

    val clConverter = new IndexToString()
      .setInputCol("clIndex")
      .setOutputCol("originalCL")

    val clConvertedDF = clConverter.transform(clIndexedDF)
    clConvertedDF.createOrReplaceTempView("cl_conv_df")

    val clIndexMaxMin = clConvertedDF
      .agg(
        max("clIndex").alias("max_index"),
        min("clIndex").alias("min_index")
      ).map(row => MaxMin(
        row.getAs[Double]("max_index"),
        row.getAs[Double]("min_index"),
        row.getAs[Double]("max_index") - row.getAs[Double]("min_index")
      ))
      .head
    val cityLevelFeatureDF = sql(spark,
      s"""
         |SELECT pdf.device, ccdf.clIndex as index, 1.0 AS cnt
         |FROM (
         |    SELECT device,  city_level
         |    FROM profile_df
         |) pdf
         |JOIN cl_conv_df ccdf
         |ON pdf.device = ccdf.device
         |AND pdf.city_level = ccdf.originalCL
       """.stripMargin)
    cityLevelFeatureDF.createOrReplaceTempView("city_level_feature")

    val preDefCellFactoryList = List(
      "HUAWEI", "OPPO", "VIVO", "XIAOMI",
      "SAMSUNG", "MEIZU", "GIONEE")
    val cfIndexMaxMin = cfConvertedDF
      .agg(
        max("cfIndex").alias("max_index"),
        min("cfIndex").alias("min_index")
      ).map(row => MaxMin(
        row.getAs[Double]("max_index"),
        row.getAs[Double]("min_index"),
        row.getAs[Double]("max_index") -  row.getAs[Double]("min_index")

      ))
      .head

    val sep = "\u0001"
    val cellFactoryFeatureDF = sql(spark,
      s"""
         |SELECT pdf.device,
         |    case when array_contains(SPLIT('${preDefCellFactoryList.mkString(sep)}', '$sep'), pdf.cell_factory)
         |    then ccdf.cfIndex
         |    else ${cfIndexMaxMin.maxV + 1}
         |    end as index, 1.0 AS cnt
         |FROM (
         |    SELECT device, cell_factory
         |    FROM profile_df
         |) pdf
         |JOIN cf_conv_df ccdf
         |ON pdf.device = ccdf.device
         |AND pdf.cell_factory = ccdf.originalCF
       """.stripMargin)
    cellFactoryFeatureDF.createOrReplaceTempView("cell_factory_feature")


    val preDefProvinceList = List(
      "广东", "浙江", "河南", "江苏", "山东", "四川",
      "河北", "湖南", "江西", "安徽", "广西", "云南",
      "湖北", "福建", "辽宁", "陕西", "山西", "北京",
      "内蒙古", "贵州", "黑龙江", "重庆", "上海", "吉林",
      "甘肃", "新疆", "天津", "海南", "宁夏", "青海",
      "台湾", "西藏")
    val pcnIndexMaxMin = pcnConvertedDF
      .agg(
        max("pcnIndex").alias("max_index"),
        min("pcnIndex").alias("min_index")
      ).map(row => MaxMin(
        row.getAs[Double]("max_index"),
        row.getAs[Double]("min_index"),
        row.getAs[Double]("max_index") - row.getAs[Double]("min_index")
      ))
      .head
    val provinceFeatureDF = sql(spark,
      s"""
         |SELECT pdf.device,
         |    case when array_contains(SPLIT('${preDefProvinceList.mkString(sep)}', '$sep'), pdf.province_cn)
         |    then pcdf.pcnIndex
         |    else ${pcnIndexMaxMin.maxV + 1}
         |    end as index, 1.0 AS cnt
         |FROM (
         |    SELECT device,
         |        province_cn
         |    FROM profile_df
         |) pdf
         |JOIN pcn_conv_df pcdf
         |ON pdf.device = pcdf.device
         |AND pdf.province_cn = pcdf.originalPCN
       """.stripMargin)
    provinceFeatureDF.createOrReplaceTempView("province_feature")


    val tagIndexMaxMin = sql(spark,
      s"""
         |SELECT
         |MAX(CAST(tag AS double)) AS max_index,
         |MIN(CAST(tag AS double)) AS min_index
         |FROM
         |(
         |    SELECT split(tag_list,'=')[0] AS tag_index
         |    FROM profile_df
         |) pdf
         |LATERAL VIEW EXPLODE(SPLIT(tag_index, ',')) s as tag
       """.stripMargin)
      .map(row => MaxMin(
        row.getAs[Double]("max_index"),
        row.getAs[Double]("min_index"),
        row.getAs[Double]("max_index") - row.getAs[Double]("min_index")
      ))
      .head


    val fullFeatureDF = sql(spark,
      s"""
         |SELECT  p_tbl.device
         |        ,split(tag_list,'=')[0] AS tag_index
         |        ,split(tag_list,'=')[1] AS tag_value
         |        ,app_index
         |        ,app_value
         |FROM profile_df p_tbl
         |JOIN (
         |        SELECT  device
         |                ,collect_list(index) AS app_index
         |                ,collect_list(cnt) AS app_value
         |        FROM (
         |                SELECT device,
         |                ${tagIndexMaxMin.DiffV + 1} + index AS index, cnt
         |                FROM city_level_feature
         |
         |                UNION ALL
         |                SELECT device,
         |                ${tagIndexMaxMin.DiffV + clIndexMaxMin.DiffV + 1} + index AS index, cnt
         |                FROM cell_factory_feature
         |
         |                UNION ALL
         |                SELECT device,
         |                ${tagIndexMaxMin.DiffV + clIndexMaxMin.DiffV + cfIndexMaxMin.DiffV + 1} + index AS index, cnt
         |                FROM province_feature
         |
         |                UNION ALL
         |                SELECT device,
         |                ${tagIndexMaxMin.DiffV + clIndexMaxMin.DiffV + cfIndexMaxMin.DiffV + pcnIndexMaxMin.DiffV + 1}
         |                 + fe_df_tbl.index AS index,
         |                dtc.cnt
         |                FROM (
         |                    SELECT device, cate_l1_id AS type, cat_cnt AS cnt
         |                    FROM app_cate_df
         |
         |                    UNION ALL
         |                    SELECT device, feature AS type, cnt
         |                    FROM in_un_df
         |                ) dtc
         |                JOIN fe_df fe_df_tbl
         |                ON dtc.type = fe_df_tbl.type
         |        ) all_tbl
         |        GROUP BY device
         |) feature_tbl
         |ON p_tbl.device = feature_tbl.device
       """.stripMargin
    )


    fullFeatureDF.createOrReplaceTempView("feature_pre")


    // 获取数据
    val trainingDataDF = sql( spark,
      """
        |SELECT device, tag_index, tag_value, app_index, app_value
        |FROM feature_pre
        |WHERE SIZE(SPLIT(tag_index,',')) >= 3
      """.stripMargin).cache()

    val maxIndex = tagIndexMaxMin.DiffV + clIndexMaxMin.DiffV +
      cfIndexMaxMin.DiffV + pcnIndexMaxMin.DiffV + feIndexMaxMin.DiffV + 6
    print(s"now print max index for debug $maxIndex")
    val trainingData = trainingDataDF.map(r => {
      (r.getString(0),
        mlVectors.dense(
          mllibVectors.sparse(maxIndex.toInt,
            concat(r.getString(1).split(",").map(x => x.toInt),
              r.getAs[Seq[Double]](3).map(m => m.toInt).toArray),
            concat(r.getString(2).split(",").map(x => x.toDouble),
              r.getAs[Seq[Double]](4).toArray)
          ).toArray
        ).toSparse
      )
    }).toDF("device", "app_features")


    // 标准化
    val scalerOP = new StandardScaler()
      .setInputCol("app_features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModelOP = scalerOP.fit(trainingData)

    // Normalize each feature to have unit standard deviation.
    val scaledDataDF = scalerModelOP.transform(trainingData).select("device", "scaledFeatures")

    // 合并
    scaledDataDF.map {
      case Row(device: String, appFeatures: DenseVector) =>
        (device, appFeatures.toArray.map(_.formatted("%.3f")).mkString(","))
      }.toDF("device", "tfidflist")
      .createOrReplaceTempView("result")

    spark.sql(
      """
        |create table if not exists test.chenfq_lookalike_feature(
        |device string,
        |tfidflist string
        |)
        |partitioned by (day string,type string)
        |stored as orc
        |
      """.stripMargin)

    spark.sql(
      s"""
        |insert overwrite table test.chenfq_lookalike_feature
        |partition (day='$endDate',type='add_app_device_feature_without_pca')
        |select  device, tfidflist
        |from  result
      """.stripMargin)

  }

}
