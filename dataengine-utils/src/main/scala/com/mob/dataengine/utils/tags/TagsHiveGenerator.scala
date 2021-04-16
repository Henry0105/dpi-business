package com.mob.dataengine.utils.tags

import com.mob.dataengine.utils.DateUtils
import com.mob.dataengine.utils.tags.deps.{AbstractDataset, DeviceProfileFull, _}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory
import scopt.OptionParser

object TagsHiveGenerator {
  @transient lazy private[this] val logger = LoggerFactory.getLogger(getClass)

  def run(param: Param): Unit = {
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName(s"TagsHiveGenerator[${DateUtils.currentDay()}]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    // 对tag1\u0001tag2\u0002value1\u0001value2 这样的数据去掉value为空的
    spark.udf.register("compact_tags", (value: String) => {
      val arr = value.split("\u0002", 2)
      val tagIds = arr(0).split("\u0001")
      val tagValue = arr(1).split("\u0001")
      val tmp = tagIds.zip(tagValue).filter{case (a, b) => StringUtils.isNotBlank(b)}
      s"${tmp.map(_._1).mkString("\u0001")}\u0002${tmp.map(_._2).mkString("\u0001")}"
    })

    val cxt = Context(
      spark = spark,
      timestampHandler = new TimestampHandler(param.updateTime),
      tableStateManager = new AndroidTableStateManager(spark),
      sample = param.sample
    )

    val deviceProfileFull = DeviceProfileFull(cxt)
    val deviceTagCodIsFull = DeviceTagCodIsFull(cxt)
    val deviceMinTime = DeviceMinTime(cxt)
    val financeAction = FinanceAction(cxt)
    val cookPortrait = DmpCookPortrait(cxt)
    val financialInstalled = FinancialInstalled(cxt)
    val appPortrait = DmpAppPortrait(cxt)
    val tourPortrait = DmpTourPortrait(cxt)
    val gamesTag = DeviceGames(cxt)
    val onlineGameActive = OnlineGameActive(cxt)
    val offlineHotel = OfflineHotel(cxt)
    val travelLabelMonthly = TravelLabelMonthly(cxt)
    val deviceOfflineCar = DeviceOfflineCar(cxt)
    val cateringStyle = CateringStyle(cxt)
    val cateringOffineStyle = CateringOffineStyle(cxt)
    val modelsConfidenceFull = DeviceModelsConfidenceFull(cxt)
    val multiloanFinance = MultiloanFinance(cxt)

    def option(x: String): Option[String] = if (StringUtils.isBlank(x)) None else Some(x)

    def joinInternal1(dataset1: AbstractDataset, dataset2: AbstractDataset): DataFrame = {
      val tmpUDF = udf((ts1: String, ts2: String) => {
        (dataset1.processTs(option(ts1)) ++ dataset2.processTs(option(ts2))).max
      })
      dataset1.dataset.join(
        dataset2.dataset, Seq("device"), "full"
      ).withColumn("update_time", tmpUDF(dataset1.timeCol, dataset2.timeCol))
        .drop(dataset1.timeCol)
        .drop(dataset2.timeCol)
    }
    /**
     * 单个dataset的time列进行处理
     */
    def processDataset(dataset1: AbstractDataset): DataFrame = {
      val tmpUDF = udf((ts1: String) => {
        dataset1.processTs(option(ts1))
      })
      dataset1.dataset.withColumn("update_time", tmpUDF(dataset1.timeCol))
        .drop(dataset1.timeCol)
    }

    val cusUDF = udf((ts1: String, ts2: String) => {
      if (StringUtils.isBlank(ts1)) {
        ts2
      } else if (StringUtils.isBlank(ts2)) {
        ts1
      } else {
        Seq(ts1, ts2).max
      }
    })

    def joinInternal2(d1: DataFrame, d2: DataFrame): DataFrame = {
      val df1 = d1.withColumnRenamed("update_time", "update_time_1")
      val df2 = d2.withColumnRenamed("update_time", "update_time_2")
      df1.join(df2, Seq("device"), "full").withColumn(
        "update_time", cusUDF(
          df1.col("update_time_1"), df2.col("update_time_2")
        ))
        .drop(df1.col("update_time_1"))
        .drop(df2.col("update_time_2"))
    }

    def joinDatasets(_seq: Seq[AbstractDataset]): DataFrame = {
      var seq: Seq[DataFrame] = _seq.sliding(2, 2).map(arr =>
        if (1 == arr.size) { // last
          processDataset(arr.head)
        } else {
          joinInternal1(arr(0), arr(1))
        }
      ).toSeq

      while (seq.size > 1) {
        seq = seq.sliding(2, 2).map(arr =>
          if (1 == arr.size) { // last
            arr.head
          } else {
            joinInternal2(arr(0), arr(1))
          }
        ).toSeq
      }

      seq.head
    }

    val dataset = joinDatasets(Seq(
      deviceMinTime, appPortrait, financeAction, financialInstalled, cookPortrait, tourPortrait,
      deviceTagCodIsFull, deviceProfileFull, offlineHotel, onlineGameActive, travelLabelMonthly,
      cateringOffineStyle, cateringStyle, deviceOfflineCar, gamesTag, modelsConfidenceFull, multiloanFinance
    ))


//    val dataset =joinInternal2(
//      joinInternal2(
//        joinInternal2(
//          joinInternal1(deviceMinTime, appPortrait),
//          joinInternal1(financeAction, financialInstalled)),
//        joinInternal2(
//          joinInternal1(cookPortrait, tourPortrait),
//          joinInternal1(imeiDeviceTagCodIsFull, deviceProfileFull))
//      ),gamesTag.dataset
//    )

    dataset.printSchema()

    dataset.createOrReplaceTempView("temp")

    val fields = spark.table(param.hiveTableName).schema.map(_.name).dropRight(1)

    spark.sql(
      s"""
         |insert overwrite table ${param.hiveTableName} partition (day=${param.updateTime})
         |select
         |  ${fields.mkString(",")}
         |from temp
           """.stripMargin)

    //    dataset.withColumn(
    //      "day", lit(params.updateTime)
    //    ) /*.repartition(1024,
    //      dataset.col("device_par")
    //    )*/ .write.partitionBy("day").mode("overwrite").format("orc").saveAsTable(params.hiveTableName)

    cxt.tableStateManager.persist()
    spark.stop()
  }

  case class Param(
                    hiveTableName: String = "",
                    updateTime: String = "",
                    sample: Boolean = false) {

    import org.json4s.jackson.Serialization.write

    implicit val _ = DefaultFormats

    override def toString: String = write(this)
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Param()
    val projectName = s"TagsHiveGenerator[${DateUtils.currentDay()}]"
    val parser = new OptionParser[Param](projectName) {
      head(s"$projectName")
      opt[String]('h', "hiveTableName")
        .text(s"hive表名")
        .required()
        .action((x, c) => c.copy(hiveTableName = x))
      opt[String]('t', "updateTime")
        .text(s"tag更新时间")
        .action((x, c) => c.copy(updateTime = x))
      opt[Boolean]('s', "sample")
        .text(s"测试")
        .action((x, c) => c.copy(sample = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        println(params)
        run(params)
      case _ => sys.exit(1)
    }
  }
}

/**
 * --conf "spark.yarn.executor.memoryOverhead=10240"  \
 * --conf "spark.sql.shuffle.partitions=10240"

 * spark2-submit --executor-memory 10g \
 *  --master yarn \
 * --executor-cores 4 \
 * --name TagsHiveGenerator[20180821] \
 * --deploy-mode cluster \
 * --class com.mob.dataengine.utils.tags.TagsHiveGenerator \
 * --driver-memory 4g \
 * --conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45" \
 * --conf "spark.memory.storageFraction=0.1" \
 * --conf "spark.speculation.quantile=0.99" \
 * --conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45" \
 * --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
 * -XX:+PrintTenuringDistribution" \
 * --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
 * --conf "spark.speculation=true" \
 * --conf "spark.sql.shuffle.partitions=3072" \
 * --conf "spark.yarn.executor.memoryOverhead=5120" \
 * --conf "spark.shuffle.service.enabled=true" \
 * --files /home/dataengine/releases/midengine/dev.v0.7.3/conf/log4j.properties,/home/dataengine/releases
 * /midengine/dev.v0.7.3/conf/hive_database_table.properties \
 * /home/dataengine/releases/midengine/dev.v0.7.3/lib/dataengine-utils-v0.7.2.1-jar-with-dependencies.jar  \
 * -h rp_dataengine.device_tags -t 20180821
 */
