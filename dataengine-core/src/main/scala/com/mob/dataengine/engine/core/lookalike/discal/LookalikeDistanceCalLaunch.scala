package com.mob.dataengine.engine.core.lookalike.discal


import com.mob.dataengine.commons.{BaseParam, DeviceSrcReader}
import org.slf4j.LoggerFactory
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.BaseJob


object LookalikeDistanceCalLaunch extends BaseJob[BaseParam]{

  private[this] val logger = LoggerFactory.getLogger(this.getClass)


  override def run(): Unit = {

    val spark = jobContext.spark

    import spark._
    import spark.implicits._

    val pacTFIDFDay = sql(
      sqlText = s"SHOW PARTITIONS ${PropUtils.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING}"
    ).collect().map(_.getString(0)).max

    val tagsMappingDF = spark.sql(
      s"""
         |SELECT
         |    device,
         |    tfidflist
         |FROM
         |    ${PropUtils.HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING}
         |WHERE
         |    $pacTFIDFDay
        """.stripMargin
    )


    jobContext.params.foreach { param =>

      val smallDeviceDF = DeviceSrcReader.toRDD2(spark, param.inputs.head).toDF("device").limit(80000000).cache()

      val lookalikeDisCal = LookalikeDistanceCal(
        spark,
        jobContext.jobCommon,
        jobContext.jobCommon.jobName,
        tagsMappingDF,
        smallDeviceDF,
        pacTFIDFDay,
        param.output
      )

      lookalikeDisCal.submit()
    }
  }
}
