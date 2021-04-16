package com.mob.dataengine.utils.tags

import com.mob.dataengine.utils.DateUtils
import com.mob.dataengine.utils.tags.deps.{AbstractDataset, DeviceProfileFull, _}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory
import scopt.OptionParser

object IosTagsHiveGenerator {
  @transient lazy private[this] val logger = LoggerFactory.getLogger(getClass)

  def run(param: Param): Unit = {
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName(s"IosTagsHiveGenerator[${DateUtils.currentDay()}]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val cxt = Context(
      spark = spark,
      timestampHandler = new TimestampHandler(param.updateTime),
      tableStateManager = new IosTableStateManager(spark),
      sample = param.sample
    )

    val idfaDeviceInfoFull = IdfaDeviceInfoFull(cxt)
    val idfaIpLocationFull = IdfaIpLocationFull(cxt)
    val iosActiveTagList = IosActiveTagList(cxt)
    val iosPermanentPlace = IosPermanentPlace(cxt)
    val iosSnsInfo = IosSnsInfo(cxt)
    val iosWorkingLivingPlace = IosWorkingLivingPlace(cxt)

    def option(x: String): Option[String] = if (StringUtils.isBlank(x)) None else Some(x)

    def joinInternal1(dataset1: AbstractDataset, dataset2: AbstractDataset): DataFrame = {
      val tmpUDF = udf((ts1: String, ts2: String) => {
        (dataset1.processTs(option(ts1)) ++ dataset2.processTs(option(ts2))).max
      })
      dataset1.dataset.join(
        dataset2.dataset, Seq("idfa"), "full"
      ).withColumn("update_time", tmpUDF(dataset1.timeCol, dataset2.timeCol))
        .drop(dataset1.timeCol)
        .drop(dataset2.timeCol)
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
      df1.join(df2, Seq("idfa"), "full").withColumn(
        "update_time", cusUDF(
          df1.col("update_time_1"), df2.col("update_time_2")
        ))
        .drop(df1.col("update_time_1"))
        .drop(df2.col("update_time_2"))
    }

    val dataset = joinInternal2(
      joinInternal2(
        joinInternal1(iosActiveTagList, iosWorkingLivingPlace),
        joinInternal1(iosPermanentPlace, iosSnsInfo)),
      joinInternal1(idfaIpLocationFull, idfaDeviceInfoFull))

    dataset.printSchema()

    dataset.createOrReplaceTempView("temp")

    spark.sql(
      s"""
         |insert overwrite table ${param.hiveTableName} partition (day=${param.updateTime})
         |select
         |  idfa                 ,
         |  carrier              ,
         |  model                ,
         |  screensize           ,
         |  sysver               ,
         |  price                ,
         |  breaked              ,
         |  public_date          ,
         |  factory              ,
         |  country              ,
         |  province             ,
         |  city                 ,
         |  area                 ,
         |  networktype          ,
         |  language             ,
         |  tag_list             ,
         |  permanent_country    ,
         |  permanent_province   ,
         |  permanent_city       ,
         |  permanent_country_cn ,
         |  permanent_province_cn,
         |  permanent_city_cn    ,
         |  gender               ,
         |  lon_home             ,
         |  lat_home             ,
         |  home_country         ,
         |  home_province_code   ,
         |  home_city_code       ,
         |  home_area_code       ,
         |  lon_work             ,
         |  lat_work             ,
         |  work_country         ,
         |  work_province_code   ,
         |  work_city_code       ,
         |  work_area_code       ,
         |  type                 ,
         |  update_time
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
    val projectName = s"IosTagsHiveGenerator[${DateUtils.currentDay()}]"
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
