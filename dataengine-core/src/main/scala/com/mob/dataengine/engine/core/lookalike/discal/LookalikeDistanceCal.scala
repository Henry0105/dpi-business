package com.mob.dataengine.engine.core.lookalike.discal

import com.mob.dataengine.commons.enums.DeviceType.DEVICE
import com.mob.dataengine.commons.enums.JobName
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.commons.{DeviceCacheWriter, JobCommon, JobOutput}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


case class LookalikeDistanceCal(
      @transient spark: SparkSession,
                 job: JobCommon,
                 jobName: String,
                 tagsMappingDF: DataFrame,
                 smallDeviceDF: DataFrame,
                 pacTFIDFDay: String,
                 output: JobOutput
) {

  private var resultDF: DataFrame = _
  private var optCache: DataFrame = _

  def submit(): Unit = {
    start()
    cal()
    insertIntoHive()
  }

  private def start(): Unit = {
    resultDF = (if (smallDeviceDF.count() > 1000000) { // 1kw
      tagsMappingDF.join(smallDeviceDF, Seq("device"))
    } else {
      tagsMappingDF.join(broadcast(smallDeviceDF), Seq("device"))
    }).toDF("device", "tfidflist")
      .groupBy("device").agg(max("tfidflist").as("tfidflist"))

    smallDeviceDF.unpersist()

    resultDF.printSchema()
    resultDF.explain(true)
    resultDF.cache()
    resultDF.createOrReplaceTempView("deviceIdTab")
  }

  private def cal(): Unit = {

    import spark.implicits._

    // 计算客户数据的中心点
    val res = spark.sql("select tfidflist from deviceIdTab").rdd.map {
      x =>
        val v = x.getString(0)
        Vectors.dense(v.split(",").map(_.toDouble))
    }

    val resCnt = res.count()

    if (0 == resCnt) {
      println("lookalike deviceIdTab is empty")
    } else {
      println(s"res cnt is $resCnt")
      val result = Statistics.colStats(res).mean.toArray.map {
        _.formatted("%.3f").toFloat
      }


      // 取样，获得分割点
      spark.sql(sqlText =
        s"""
           |SELECT
           |    device,
           |    tfidflist
           |FROM
           |${PropUtils.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_DEMO}
         """.stripMargin
      ).map {
        x =>
          (x.getString(0), math.sqrt(x.getString(1).split(",").map {
            _.toFloat
          }.zip(result).
            map(p => p._1 - p._2).map(d => d * d).sum))
      }.toDF(colNames = "device", "score").createOrReplaceTempView("tmpTab")

      val tmp2 = spark.sql(sqlText =
        s"""
           |SELECT
           |    score,
           |    rank() over (order by score) as rank
           |FROM
           |    tmpTab
         """.stripMargin
      ).cache()
      tmp2.createOrReplaceTempView("tmp2")
      val cnt = tmp2.count()

      val tt = spark.sql("select score,rank from tmp2 where rank%"
        + (cnt / 100) + "=0 and rank<=" + (cnt * 0.2)).map(x => x.getDouble(0)).collect()

      spark.sql(sqlText = "uncache table tmp2")
      spark.catalog.dropTempView(viewName = "tmp2")

      // 计算我们最近60天的特征数据和用户上传数据中心点的距离
      spark.sql(
        s"""
           |SELECT
           |    device,
           |    tfidflist
           |FROM
           |    ${PropUtils.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING}
           |where
           |    $pacTFIDFDay
           """.stripMargin
      ).map {
        x =>
          (x.getString(0), math.sqrt(x.getString(1).split(",").map {
            _.toDouble
          }.zip(result).map(p => p._1 - p._2).map(d => d * d).sum))
      }.toDF("device", "score").createOrReplaceTempView("tmp")

      // join辨别出用户数据,decile 设置为0

      optCache = spark.sql(
        s"""
           |select concat_ws('\u0001', aa.device, cast(score as string), cast(case
           |  when bb.device is not null then 0
           |  when score< ${tt(0)} then 1
           |  when score< ${tt(1)} then 2
           |  when score< ${tt(2)} then 3
           |  when score< ${tt(3)} then 4
           |  when score< ${tt(4)} then 5
           |  when score< ${tt(5)} then 6
           |  when score< ${tt(6)} then 7
           |  when score< ${tt(7)} then 8
           |  when score< ${tt(8)} then 9
           |  when score< ${tt(9)} then 10
           |end as string)) as data
           |from (
           |    select
           |        device,
           |        score
           |    from
           |        tmp
           |    where
           |        score<= ${tt(9)}
           |) aa
           |left join deviceIdTab bb
           |on aa.device=bb.device
          """.stripMargin)
        .coalesce(numPartitions = 100)


      spark.sql(sqlText = "uncache table deviceIdTab")
      spark.catalog.dropTempView(viewName = "deviceIdTab")
    }
  }

  private def insertIntoHive(): Unit = {
    if (optCache != null) {
      DeviceCacheWriter.insertTable2(spark, job,
        output, optCache, biz = s"${JobName.getId(jobName)}|${DEVICE.id}")
    }
  }
}
