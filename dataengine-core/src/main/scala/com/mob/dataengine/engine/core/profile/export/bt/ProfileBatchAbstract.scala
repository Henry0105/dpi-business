package com.mob.dataengine.engine.core.profile.export.bt

import com.mob.dataengine.commons.pojo.OutCnt
import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.traits.Watching
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ArrayBuffer

/**
 * @author juntao zhang
 */
trait ProfileBatchAbstract extends Watching {
  @transient var unionDF: DataFrame = _
  @transient var finalDF: DataFrame = _
  @transient var outCnt: OutCnt = _
  @transient var matchCnt: Long = _
  @transient val inIdsFull: ArrayBuffer[String] = ArrayBuffer[String]()
  val outputHDFS: Option[String] = None
  val outputUUID: String
  val outputHDFSLanguage: String = "en"
  val sep: String = "\t"
  val limit: Option[Int] = None

  def submit(): Unit = {
    start()
    cal()
    sendMatchInfo(inIdsFull)
    insertInto()
    export(inIdsFull)
    stop()
  }

  def start(): Unit

  def cal(): Unit

  def insertInto(): Unit = {
    finalDF.createOrReplaceTempView("t")
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_SINGLE_PROFILE_TRACK_INFO} partition(uuid='$outputUUID')
         |select device,day,features,data from t
      """.stripMargin)
  }

  def stop(): Unit = {
    finalDF.unpersist()
  }

  def export(allProfileIds: Seq[String], splitThreshold: Int = 20): Unit = {
    if (outputHDFS.isDefined && outputHDFS.get.nonEmpty) {
      finalDF = finalDF.drop("data")
      // ProfileBatchSplitter根据profile id数量对结果数据集进行分组
      val outputs: Seq[(DataFrame, Seq[String], String)] = new ProfileBatchSplitter(spark, splitThreshold)
        .trySplitByProfile(finalDF, allProfileIds)
        .map{m => m.toSeq.map{case (catId, (df, partialProfileIds)) =>
          println(catId)
          val expandedProfileIds = partialProfileIds.flatMap{ id =>
            allProfileIds.filter(fullId => fullId.substring(0, fullId.indexOf("_")).equals(id))
          }
          (df, expandedProfileIds, s"${outputHDFS.get}/$catId")}}
        .getOrElse(Seq((finalDF, allProfileIds, s"${outputHDFS.get}/0")))

      // 无论上一步分组与否，统一将结果写到HDFS的CSV文件中
      outputs.foreach{case (df, curProfileIds, outputHDFSPath) =>
        val filteredDF = df.filter("size(features) > 0")
        println(curProfileIds.mkString(","))
        val profileNameMap = spark.sparkContext.broadcast(MetadataUtils.findProfileName(curProfileIds))

        spark.udf.register("transfer", (device: String, day: String, features: Map[String, Seq[String]]) => {
          (Seq(device, day) ++ curProfileIds.map(p => {
            val arr = features.get(p)
            if (arr.isDefined) {
              arr.get(1)
            } else {
              ""
            }
          })).mkString(sep)
        }
        )

        val header = if (outputHDFSLanguage == "cn") {
          (Seq("MOB设备ID", "日期") ++ curProfileIds.map(p => {
            profileNameMap.value.getOrElse(p.split("_")(0), null)
          })).mkString(sep)
        } else {
          (Seq("device", "day") ++ curProfileIds).mkString(sep)
        }

        val finalDFLimit = limit.map(filteredDF.limit).getOrElse(filteredDF)
        finalDFLimit
          .selectExpr("transfer(device,day,features)").toDF(header)
          .coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .option("quote", "\u0000")
          .csv(outputHDFSPath)
      }
    }
  }

  def sendMatchInfo(inIdsFull: Seq[String]): Unit = {
    val profileIds = spark.sparkContext.broadcast(inIdsFull)
    import spark.implicits._
    val cntDF = finalDF.mapPartitions(iter => {
      val cntArrayMap = scala.collection.mutable.Map[String, Long]()
      for (pid <- profileIds.value) {
        cntArrayMap(pid) = 0.toLong
      }

      iter.foreach(row => {
        val tag = row.getAs[Map[String, Seq[String]]]("features")
        for (pid <- profileIds.value) {
          if (tag.contains(pid) && tag(pid) != null && tag(pid).nonEmpty) {
            cntArrayMap(pid) += 1
          }
        }
      })

      cntArrayMap.toIterator
    }).toDF("pid", "cnt").cache()

    cntDF.createOrReplaceTempView("cnt_df")

    val pidCntTuple = sql("SELECT pid, sum(cnt) AS scnt FROM cnt_df GROUP BY pid")
      .map(row =>
        (row.getAs[String]("pid"), row.getAs[Long]("scnt"))
      ).collect()

    val outCntTMP = new OutCnt
    pidCntTuple.foreach(pidCnt => outCntTMP.set(s"${pidCnt._1}", pidCnt._2))
    outCnt = outCntTMP
    matchCnt = finalDF.count()
  }

  def tryBloomLoad(dataFrame: DataFrame, deviceDF: DataFrame): DataFrame = {
    import spark.implicits._
    logger.info(
      s"""
         |---------------------------------------------------------------
         |Job[${this.getClass.getSimpleName}].TableBloomLoad
         |---------------------------------------------------------------
       """.stripMargin
    )

    val deviceBF = Some(spark.sparkContext.broadcast(
      deviceDF.stat.bloomFilter($"device", deviceDF.count(), 0.001)))


    dataFrame.filter(r =>
      deviceBF.get.value.mightContainString(
        r.getAs[String]("device")
      )
    )
  }

  def max(d1: String, d2: String): String = {
    if (d1 > d2) d1 else d2
  }
}
