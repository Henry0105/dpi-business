package com.mob.dataengine.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import breeze.linalg.{DenseMatrix, DenseVector => BDV}
import com.mob.dataengine.commons.utils.{PropUtils => PU}

object PCATfidf {
  def main(args: Array[String]) {
    val day = args(0).trim
    val day1 = args(1).trim
    val day60 = args(2).trim
    val pcaPath = args(3).trim
    println(args.mkString(","))

    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    // 从rp_sdk_dmp.rp_device_profile_full取出上次更新后的新数据

    val rawData = spark.sql(s"select device,tag_list from ${PU.HIVE_TABLE_RP_DEVICE_PROFILE_FULL} " +
      s" where length(tag_list)>3 and processtime>" + day1)
    val rdd2 = rawData.map { x =>
      val k = x.getString(0)
      val v = x.getString(1)
      val idx = v.split('=')(0).split(',').toArray.map(_.toInt)
      val value = v.split('=')(1).split(',').toArray.map(_.toDouble)
      (k, Vectors.sparse(850, idx, value), day)
    }.rdd

    // 对数据进行降维处理

    val data = rdd2.map { case (s, v, z) => v }
    val data_norm = new StandardScaler(withMean = true, withStd = true).fit(data)
    val scaler3 = new StandardScalerModel(data_norm.std, data_norm.mean)
    val pca_matrix = sc.textFile(pcaPath).flatMap(_.split(",")).map(_.toDouble).collect()
    val tmp_matrix = new DenseMatrix(850, 624, pca_matrix)
    val pcLoad = sc.broadcast(tmp_matrix)
    rdd2.map {
      case (s, v, z) =>
        val dv = scaler3.transform(v.toDense).toArray
        val bdv = BDV(dv)
        val rsl = (bdv.toDenseMatrix * pcLoad.value).map { x => x.toDouble.formatted("%.3f") }
        (s, rsl.toArray.mkString(","), z)
    }.toDF("device", "tfidflist", "processTime").createOrReplaceTempView("tmpTab")

    // 取出最近60天的数据和rp_dataengine.rp_mobeye_tfidf_pca_tags_mapping join 获取用户的画像属性

    spark.sql(
      s"""
         |insert overwrite table ${PU.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING} partition(day=$day)
         | select
         |   c.device,imei,mac,phone,tfidflist,country,province,city,gender,agebin,segment,edu,kids,income,
         |   cell_factory,model,model_level,carrier,network,screensize,sysver,occupation,house,repayment,
         |   car,workplace,residence,applist,married,match_flag,c.processtime
         | from
         | (
         |    select device,tfidflist,processtime
         |    from
         |    (
         |      select device,tfidflist,processtime,row_number()over(partition by device order by processtime desc) rank
         |      from
         |      (
         |        select device,tfidflist,processtime from ${PU.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING}
         |        where day=$day1 and processtime>$day60
         |        union all
         |        select device,tfidflist,processtime from tmpTab
         |      )a
         |    ) aa where rank=1
         | ) c join ${PU.HIVE_TABLE_DEVICE_ID_TAGS_MAPPING} b on c.device=b.device
      """.stripMargin)

    // 从结果中取出1%的样本数据

    spark.sql(s"insert overwrite table ${PU.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_DEMO} select device,tfidflist from " +
      s" ${PU.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING} where day=" + day + " and rand()<=0.01")
  }
}
