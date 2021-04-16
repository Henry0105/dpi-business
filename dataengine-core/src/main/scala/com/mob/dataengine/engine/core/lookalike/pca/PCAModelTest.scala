package com.mob.dataengine.engine.core.lookalike.pca

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.ml.feature.{PCA, StandardScaler}
import org.apache.spark.ml.linalg.{Vector, Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vectors => mllibVectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object PCAModelTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("lookalikeSemiSupervised")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val input = Array(
      ("1", Array(1, 3, 9), Array(2.0, 2, 4)),
      ("2", Array(2, 4, 9), Array(2.0, 2, 4)),
      ("3", Array(2, 3, 8), Array(2.0, 2, 4)),
      ("4", Array(2, 3, 7), Array(2.0, 2, 4)),
      ("5", Array(1, 2, 9), Array(2.0, 2, 4)),
      ("6", Array(4, 6, 9), Array(2.0, 2, 4)),
      ("7", Array(5, 8, 9), Array(2.0, 2, 4)),
      ("8", Array(6, 7, 9), Array(2.0, 2, 4))
    )

    val sourceData = spark.sparkContext.parallelize(input)

    println(sourceData.collect().mkString(","))

    val trainingData = sourceData.map {
      case (device: String, index: Array[Int], value: Array[Double]) =>
        (device,
          mlVectors.dense(
            mllibVectors.sparse(10, index, value).toArray
          ).toSparse
        )
    }.toDF("device", "features")

    // 标准化
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")
      .setWithStd(true).setWithMean(true)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(trainingData)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(trainingData).drop("features")

    scaledData.show(false)

    // PCA降维
    val pca = new PCA().setInputCol("scaledFeatures").setOutputCol("pcaFeatures").setK(5).fit(scaledData)

    val pc = Array(0.0, -0.011627568998648533, 0.36581392483958064, 0.47147409677660745, -0.3102047023053731,
      -0.09227503599291904, -0.3574932132799838, 0.23483493723655502, 0.289899392343484, -0.5215601648249233, 0.0,
      -0.1964876408687235, 0.15233843689499332, 0.0477876397622058, 0.26404905944032586, -0.5535976068875297,
      0.3617170009450263, 0.47775150363211905, -0.4151163611253049, -0.16823332518839218, 0.0, 0.7358485261861104,
      0.14264097283006927, 0.12277371528895856, -0.11530936890242283, -0.3477674299295796, -0.2642082674202465,
      -0.14593761392629578, -0.3819038102225143, 0.2278472346623587, 0.0, 0.1357360987748134, 0.5246859638387636,
      0.16777007083241458, -0.5916911644776828, 0.20149861318579174, 0.22479456604092943, 0.4471567447187667,
      -0.1881069969525122, 0.03959084950602429, 0.0, -0.10733502839983666, 0.37165680233036125, -0.39921433641821635,
      -0.03662220178267668, 0.4317205654632905, -0.41560759191915086, 0.41246044966450096, -0.3870144144486994,
      0.10305845070365997
    )

    val pcDM = spark.sparkContext.broadcast(
      new DenseMatrix(10, 5, pc
      )
    )

    scaledData.map {
      case Row(d: String, v: Vector) =>
        d -> (new DenseVector(v.toArray).toDenseMatrix * pcDM.value).map(_.toDouble).toArray.mkString(",")
    }.toDF("device", "tfidflist")
      .show(false)

    val result = pca.transform(scaledData).select("device", "pcaFeatures").persist(StorageLevel.DISK_ONLY)

    println("pca.pc => " + spark.sparkContext.parallelize(pca.pc.values).collect().mkString(","))

    println("pca.explainedVariance => " + pca.explainedVariance.toArray.mkString(","))
    /**
     * device|tfidflist                                                                                          |
     * +------+---------------------------------------------------------------------------------------------------+
     * |1     |0.055405647271273284,-0.6677883533473278,1.9240552724099949,0.9310405449261596,-0.8692243940996437 |
     * |2     |-0.8161939938378406,0.5197492522219451,0.1149795383717514,-1.946142075027086,0.7501943604470596    |
     * |3     |2.6708724869315,-0.7107703854698758,-0.9174989754646163,-0.934954296454384,-1.2050077575670044     |
     * |4     |2.522854494952544,1.6893352396069763,-0.28320154197773867,0.7726892866270141,0.9440494481002123    |
     * |5     |-0.17119366735956087,-0.4751237267124949,1.9536907421955598,-0.37471958499645236,0.5974371959946305|
     * |6     |-2.27284278643763,1.0161481631498934,-0.7226323769601131,-0.4789327545526666,-0.8429268612842719   |
     * |7     |-0.7018063973503565,-2.459405759639816,-1.3997105583628144,0.6307547333813441,0.8349272875408926   |
     * |8     |-1.2870957841699289,1.0878555701906996,-0.6696821002120227,1.4002641460960712,-0.20944927913187522
     */
    result.map(r => {
      r.getString(0) -> r.getAs[Vector](1).toArray.mkString(",")
    }).toDF("device", "tfidflist")
      .show(false)

    spark.stop()
  }
}
