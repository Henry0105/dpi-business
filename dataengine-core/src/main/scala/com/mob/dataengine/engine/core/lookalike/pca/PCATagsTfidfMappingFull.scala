package com.mob.dataengine.engine.core.lookalike.pca

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{PCA, StandardScaler}
import org.apache.spark.ml.linalg.{Vector, Vectors => mlVectors}
import org.apache.spark.mllib.linalg.{Vectors => mllibVectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.Array.concat


case class PCATagsTfidfMappingFull(jc: JobContext) extends AbstractPCAJob {
  @transient private[this] val logger: Logger = Logger.getLogger(this.getClass)
  @transient private[this] val spark: SparkSession = jc.spark

  /** private lazy val createTableExplainedVariance =
   * s"create table if not exists ${PropUtils.HIVE_TARGET_TABLE_RP_DATAENGINE_PCA_EXPLAINED_VARIANCE} " +
   *   s"(vars double) partition (day string) stored as orc"
   * private lazy val createTablePcaMatrixPc =
   * s"create table if not exists ${PropUtils.HIVE_TARGET_TABLE_RP_DATAENGINE_PCA_MATRIX_PC} " +
   *   s"(vars double) partition (day string) stored as orc"
   * private lazy val createTablePcaTagsTfidfMapping =
   * s"create table if not exists ${PropUtils.HIVE_TARGET_TABLE_RP_DATAENGINE_PCA_TAGS_TFIDF_MAPPING} " +
   *   s"(device string, tfidflist string) partition (day string) stored as orc"
   */

  /**
   * 根据增量表获取目前profile_full表中最终数据的日期
   * @return
   */
  private def getRankDate: String = {
    spark.sql(s"show partitions ${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_INCR}")
      .collect().map(_.getAs[String](0)).last.split("=")(1)
  }



  def submit(): Unit = {
    import spark.implicits._

    val rankDate: String = getRankDate
    val _60DaysBefore = genAnotherDay(rankDate, -60)
    logger.info(s"$currTime|pcamodel模型全量计算开始|rankDate:$rankDate")

    // 获取数据
    val trainingDataOrigin = spark.sql(
      s"""
        | select  device,tag_index,tag_value,app_index,app_value
        | from (
        | select  device, tag_list, applist
        | from    ${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL}
        | where   processtime >= '${_60DaysBefore}'
        | ) t
        | explode_tags(tag_list) tl as tag_name, tag_value
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
    val scaledData = scalerModel.transform(trainingData)

    // scaledData.cache()
    // PCA降维
    val pca = new PCA().setInputCol("scaledFeatures").setOutputCol("pcaFeatures").setK(800).fit(scaledData)

    val result = pca.transform(scaledData).select("device", "pcaFeatures")// .persist(StorageLevel.DISK_ONLY)

    result.map(r => {
      r.getString(0) -> r.getAs[Vector](1).toArray.mkString(",")
    }).toDF("device", "tfidflist").createOrReplaceTempView("result")

    /* 保存训练结果 */
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_RP_DATAENGINE_PCA_TAGS_TFIDF_MAPPING}
         |partition (day='$rankDate')
         |select   device,tfidflist
         |from     result
      """.stripMargin)

    /* 保存pc */
    spark.sparkContext.parallelize(pca.pc.values, 1)

    /* 保存explainedVariance */
    spark.sparkContext.parallelize(pca.explainedVariance.toArray, 1).toDF("vars")
      .createOrReplaceTempView("explained_variance_tmp")
    spark.sql(s"insert overwrite table ${PropUtils.HIVE_TABLE_RP_DATAENGINE_PCA_EXPLAINED_VARIANCE} " +
      s"partition (day='$rankDate') select vars from explained_variance_tmp")
    /* result.show()
     * 选择PCA K值
     * var i = 1
     * for (x <- pca.explainedVariance.toArray) {
     * println("---------"+i+s":$x")
     * i=i+1
     * }
     */

    spark.stop()
  }
}
