package com.mob.dataengine.engine.core.lookalike.pca

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.{Vector, Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vectors => mllibVectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.Array.concat


/**
 * Created by liuhy on 2018/7/18.
 */
object PCAModelTransform {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("PCAModelTransform")
      .getOrCreate()

    import spark.implicits._

    val pcDM = spark.sparkContext.broadcast(
      new DenseMatrix(910, 800,
        spark.sparkContext.textFile(
          "/tmp/pca_pc2", 1
        ).collect().map(_.toDouble)
      )
    )
    // 获取数据
    val trainingDataOrigin = spark.sql(
      """
        | select  device,tag_index,tag_value,app_index,app_value
        | from    test.liuhy_fin_pca_feature
        | where   size(tag_value)>=3
        | limit 100
      """.stripMargin)

    val trainingData = trainingDataOrigin.map(r => {
      val v = r.getSeq[String](1).map(_.toInt).toArray
      val u = r.getSeq[String](2).map(_.toDouble).toArray
      (r.getString(0),
        mlVectors.dense(
          mllibVectors.sparse(910, concat(v, r.getSeq[Int](3).toArray)
            , concat(u, r.getSeq[Double](4).toArray)).toArray
        ).toSparse
      )
    }
    ).toDF("device", "features").persist(StorageLevel.DISK_ONLY)


    // 标准化
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")
      .setWithStd(true).setWithMean(true)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(trainingData)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(trainingData).drop("features")

    scaledData.map {
      case Row(d: String, v: Vector) =>
        d -> (new DenseVector(v.toArray).toDenseMatrix * pcDM.value).map(_.formatted("%.3f")).toArray.mkString(",")
    }.toDF("device", "tfidflist").createOrReplaceTempView("tmpTab")

    spark.sql(
      s"""
         |insert overwrite table test.liuhy_pca_add_feature2
         |partition (day='20180501')
         |select    device, tfidflist
         |from      tmpTab
       """.stripMargin
    )

    spark.stop()
  }
}
