package com.mob.dataengine.engine.core.location

import com.mob.dataengine.commons.{DeviceCacheWriter, JobCommon, JobOutput}
import com.mob.dataengine.commons.enums.{DeviceType, JobName}
import com.mob.dataengine.commons.pojo.{MatchInfo, OutCnt}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.constant.AreaTypeEnum._
import com.mob.dataengine.engine.core.constant.LocationDataTypeEnum._
import com.mob.dataengine.engine.core.constant.{AreaTypeEnum, LocationDataTypeEnum}
import com.mob.dataengine.engine.core.jobsparam.{JobContext, LocationDeviceMappingParam}
import com.mob.dataengine.rpc.RpcClient
import com.youzu.mob.scala.udf.IsPointInPolygonUDF
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: zhangnw
 * Date: 2018-07-27
 * Time: 16:36
 */
object LocationDataParser extends Serializable {

  private[this] val logger: slf4j.Logger = LoggerFactory.getLogger(LocationDataParser.getClass)
  // 输出表


//  def apply: LocationDataParser = instance

  /**
   *
   * @param jobContext 地理围栏任务参数
   * @return outTableName
   */
  def dataParsing(jobContext: JobContext[LocationDeviceMappingParam]): Unit = {
    val spark = jobContext.spark
    val locationTaskJob = jobContext.params
    val instance = new LocationDataParser(logger, spark)
    // 获取最后一个分区
    val homeWorkQuarter = spark.sql(s"show partitions ${PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK}")
      .collect.toList.last.getString(0)
    val freqQuarter = spark.sql(s"show partitions ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}")
      .collect.toList.last.getString(0)

    for (itemIndex <- locationTaskJob.indices) {

      val item = locationTaskJob(itemIndex)
      val areaType = item.input.areaType // 数据来源方式 1 绘制多边形  2 选择区
      val locationCode = item.input.locationCode
      val daily_monthly = item.input.dailyOrMonthly // 选择常驻数据 活动数据
      val latLonList = item.input.latLonList
      val beginDay = item.input.beginDay
      val endDay = item.input.endDay

      val uuid = item.output.uuid

      // 根据选择Device的方式分类
      AreaTypeEnum(areaType) match {
        // 1绘制
        case LAT_LON_LIST =>
          LocationDataTypeEnum(daily_monthly) match {
            // 使用常驻地数据
            case MONTHLY =>
              logger.info(
                s"""
                   |----------------------------------------------------
                   |$LAT_LON_LIST $MONTHLY
                   |data parsing...
                   |----------------------------------------------------
                  """.stripMargin
              )
              if (latLonList.isEmpty) {
                throw new IllegalArgumentException("latLonList is empty!")
              }

              instance.dataParsingLLMonthly(
                uuid,
                latLonList.getOrElse(StringUtils.EMPTY),
                homeWorkQuarter, freqQuarter, itemIndex, jobContext.jobCommon)
            // 使用流动数据
            case DAILY =>
              logger.info(
                s"""
                   |----------------------------------------------------
                   |$LAT_LON_LIST $DAILY
                   |data parsing...
                   |----------------------------------------------------
                  """.stripMargin
              )
              if (latLonList.isEmpty || beginDay.isEmpty || endDay.isEmpty) {
                throw new IllegalArgumentException("latLonList.isEmpty || beginDay.isEmpty || endDay.isEmpty")
              }
              instance.dataParsingLLDaily(
                uuid,
                latLonList.getOrElse(StringUtils.EMPTY),
                beginDay.getOrElse(StringUtils.EMPTY),
                endDay.getOrElse(StringUtils.EMPTY),
                itemIndex, jobContext.jobCommon)
          }
        // 2选择区
        case AREA_CODE =>
          LocationDataTypeEnum(daily_monthly) match {
            // 使用常驻地数据
            case MONTHLY =>
              logger.info(
                s"""
                   |----------------------------------------------------
                   |$AREA_CODE $MONTHLY
                   |data parsing...
                   |----------------------------------------------------
                  """.stripMargin
              )
              if (locationCode.isEmpty) {
                throw new IllegalArgumentException("areaCode.isEmpty")
              }
              instance.dataParsingAreaMonthly(uuid,
                locationCode.getOrElse(StringUtils.EMPTY),
                homeWorkQuarter, freqQuarter, itemIndex,
                jobContext.jobCommon)
            // 使用流动数据
            case DAILY =>
              logger.info(
                s"""
                   |----------------------------------------------------
                   |$AREA_CODE $DAILY
                   |data parsing...
                   |----------------------------------------------------
                  """.stripMargin
              )
              if (locationCode.isEmpty || beginDay.isEmpty || endDay.isEmpty) {
                throw new IllegalArgumentException("areaCode.isEmpty || beginDay.isEmpty || endDay.isEmpty")
              }
              instance.dataParsingAreaDaily(uuid,
                locationCode.getOrElse(StringUtils.EMPTY),
                beginDay.getOrElse(StringUtils.EMPTY),
                endDay.getOrElse(StringUtils.EMPTY), itemIndex,
                jobContext.jobCommon)
          }
      }
    }
  }
}

class LocationDataParser(logger: Logger, spark: SparkSession) extends Serializable {

  /**
   * 绘制一片区域之后，使用常驻数据处理device,写入clean表
   */
  private def dataParsingLLMonthly(
    uuid: String,
    llStr: String,
    lastPartitionHomeWork: String,
    lastPartitionFreq: String,
    itemIndex: Int,
    jobCommon: JobCommon): Unit = {

    //    val s = "[104.07037,30.599452],[104.073946,30.599483],[104.074053,30.594991],[104.070442,30.59496]"
    import spark.implicits._
    val latNumPattern = """[0-9]+\.[0-9]+\]""".r
    val lonNumPattern = """\[[0-9]+\.[0-9]+""".r

    // 从绘制区域中找到 经纬度的边界值
    val lats = latNumPattern.findAllIn(llStr).map(_.replaceAll("\\]", "").toDouble).toArray
    val lons = lonNumPattern.findAllIn(llStr).map(_.replaceAll("\\[", "").toDouble).toArray
    val latMax = lats.max
    val latMin = lats.min
    val lonMax = lons.max
    val lonMin = lons.min

    logger.info(
      s"""
         |----------------------------------------------------
         |lat lon list side
         |latMax $latMax
         |latMin $latMin
         |lonMax $lonMax
         |lonMin $lonMin
         |----------------------------------------------------
      """.stripMargin
    )


    val unionSQL =
      s"""
         |    SELECT device
         |       ,home_lat, home_lon
         |       ,work_lat, work_lon
         |       ,freq_lat, freq_lon
         |    FROM(
         |       SELECT device
         |           ,lat_home AS home_lat, lon_home AS home_lon
         |           ,lat_work AS work_lat, lon_work AS work_lon
         |           ,NULL AS freq_lat, NULL AS freq_lon
         |       FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK}
         |       WHERE $lastPartitionHomeWork
         |       AND ( lat_home IS NOT NULL AND lon_home IS NOT NULL
         |            AND lat_home <= '$latMax' AND lat_home >='$latMin'
         |            AND lon_home <= '$lonMax' AND lon_home >= '$lonMin')
         |         OR (lat_work IS NOT NULL AND lon_work IS NOT NULL
         |            AND lat_work <= '$latMax' AND lat_work >='$latMin'
         |            AND lon_work <= '$lonMax' AND lon_work >= '$lonMin')
         |
         |       UNION ALL
         |       SELECT device
         |           ,NULL AS home_lat, NULL AS home_lon
         |           ,NULL AS work_lat, NULL AS work_lon
         |           ,lat AS freq_lat, lon AS freq_lon
         |       FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}
         |       WHERE $lastPartitionFreq
         |       AND lat IS NOT NULL AND lon IS NOT NULL
         |           AND lat <= '$latMax' AND lat >= '$latMin'
         |           AND lon <= '$lonMax' AND lon >= '$lonMin'
         |    ) t
      """.stripMargin



    logger.info(
      s"""
         |---------------------------------------------------
         |get info ( device lat lon area ):
         |$unionSQL
         |---------------------------------------------------
      """.stripMargin
    )
    spark.sql(unionSQL)
      //      .coalesce(1000)
      .createOrReplaceTempView("inDF_dataParsingLLMonthly_tmp")


    val isInSQL =
      s"""
         |SELECT device
         |FROM inDF_dataParsingLLMonthly_tmp
         |WHERE (home_lat IS NOT NULL AND home_lat <> ''
         |  AND home_lon IS NOT NULL AND home_lon <> '' AND is_in(concat_ws(',',home_lat,home_lon),'$llStr') = '1')
         |  OR (work_lat IS NOT NULL AND work_lat <> '' AND work_lon IS NOT NULL AND work_lon <> ''
         |  AND is_in(concat_ws(',',work_lat,work_lon),'$llStr') = '1')
         |  OR (freq_lat IS NOT NULL AND freq_lat <> '' AND freq_lon IS NOT NULL AND freq_lon <> ''
         |  AND is_in(concat_ws(',',freq_lat,freq_lon),'$llStr') = '1')
      """.stripMargin

    logger.info(
      s"""
         |
         |---------------------------------------------------
         |is in the polygon SQL:
         |$isInSQL
         |---------------------------------------------------
         |
      """.stripMargin)
    // 过滤出有用的device
    spark.udf.register("is_in", IsPointInPolygonUDF.isIn, StringType)
    val filterDF = spark.sql(isInSQL)
      // 去重
      .map(row => row.getString(0)).distinct()
      .map(x => (x, uuid))
      .toDF("data", "uuid")

    // 发送count信息
    if (jobCommon.hasRPC()) {
      val deviceCnt = filterDF.count()
      println(s"device mapping cnt $deviceCnt")
      sendMatchInfo(jobCommon.rpcHost, jobCommon.rpcPort, jobCommon.jobId, uuid, deviceCnt)
    }

    saveDeviceToHive(spark, uuid, filterDF, jobCommon)
  }

  /**
   * 绘制一片区域之后，使用流动数据处理device,写入clean表
   */
  def dataParsingLLDaily(uuid: String, llStr: String, beginDay: String, endDay: String, itemIndex: Int,
    jobCommon: JobCommon): Unit = {

    //    val s = "[104.07037,30.599452],[104.073946,30.599483],[104.074053,30.594991],[104.070442,30.59496]"
    import spark.implicits._
    val latNumPattern = """[0-9]+\.[0-9]+\]""".r
    val lonNumPattern = """\[[0-9]+\.[0-9]+""".r

    // 获取绘制区域经纬度的边界值
    val lats = latNumPattern.findAllIn(llStr).map(_.replaceAll("\\]", "").toDouble).toArray
    val lons = lonNumPattern.findAllIn(llStr).map(_.replaceAll("\\[", "").toDouble).toArray
    val latMax = lats.max
    val latMin = lats.min
    val lonMax = lons.max
    val lonMin = lons.min

    // 获取基础数据
    val deviceDailySQL =
      s"""
         |SELECT
         |device,lat,lon
         |FROM ${PropUtils.HIVE_TABLE_DEVICE_STAYING_DAILY}
         |WHERE plat='1'
         |AND day>='$beginDay'
         |AND day<='$endDay'
         |AND lat IS NOT NULL
         |AND lon IS NOT NULL
         |AND lat <= '$latMax' AND lat >='$latMin' AND lon <= '$lonMax' AND lon >= '$lonMin'
      """.stripMargin
    logger.info(
      s"""
         |------------------------------------------
         |deviceDailySQL:
         |$deviceDailySQL
         |------------------------------------------
      """.stripMargin)

    spark.sql(deviceDailySQL)
      //      .coalesce(1000)
      .createOrReplaceTempView("inDF_dataParsingLLDaily_tmp")

    val isInSQL =
      s"""
         |
         | SELECT device
         | FROM inDF_dataParsingLLDaily_tmp
         | WHERE is_in(concat_ws(',',lat,lon),'$llStr') = 1
         |
      """.stripMargin
    logger.info(
      s"""
         |------------------------------------------
         |is in SQL:
         |$isInSQL
         |------------------------------------------
         |
      """.stripMargin
    )
    // 过滤出有用的device
    val filterDF = spark.sql(isInSQL)
      // 去重
      .map(row => row.getString(0)).distinct()
      .map(x => (x, uuid))
      .toDF("data", "uuid")

    if (jobCommon.hasRPC()) {
      val deviceCnt = filterDF.count()
      println(s"device mapping cnt $deviceCnt")
      sendMatchInfo(jobCommon.rpcHost, jobCommon.rpcPort, jobCommon.jobId, uuid, deviceCnt)
    }

    saveDeviceToHive(spark, uuid, filterDF, jobCommon)
  }

  /**
   * 选择area,使用常驻数据计算device，写入clean
   */
  def dataParsingAreaMonthly(uuid: String,
    areaCode: String,
    lastPartitionHomeWork: String,
    lastPartitionFreq: String,
    itemIndex: Int,
    jobCommon: JobCommon): Unit = {

    import spark.implicits._

    val filterSQL =
      s"""
         | SELECT device
         | FROM (
         |    SELECT device
         |    FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK}
         |    WHERE $lastPartitionHomeWork
         |        AND (area_home = '$areaCode' OR city_home = '$areaCode' OR
         |          area_work = '$areaCode' OR city_work='$areaCode')
         |    GROUP BY device
         |
         |    UNION ALL
         |    SELECT device
         |    FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}
         |    WHERE $lastPartitionFreq
         |        AND (area = '$areaCode' OR city = '$areaCode')
         |    GROUP BY device
         | ) t
         | GROUP BY device
      """.stripMargin

    logger.info(
      s"""
         |
         |------------------------------------------
         |filter SQL:
         |$filterSQL
         |------------------------------------------
         |
       """.stripMargin)


    val filterDF = spark.sql(filterSQL)
      //      .coalesce(1000)
      .map(row => row.getString(0)).distinct()
      .map(x => (x, uuid))
      .toDF("data", "uuid")

    if (jobCommon.hasRPC()) {
      val deviceCnt = filterDF.count()
      println(s"device mapping cnt $deviceCnt")
      sendMatchInfo(jobCommon.rpcHost, jobCommon.rpcPort, jobCommon.jobId, uuid, deviceCnt)
    }

    saveDeviceToHive(spark, uuid, filterDF, jobCommon)
  }

  /**
   * 选择area,流动数据,计算device,写入clean
   */
  def dataParsingAreaDaily(
    uuid: String,
    areaCode: String,
    beginDay: String,
    endDay: String,
    itemIndex: Int,
    jobCommon: JobCommon): Unit = {
    import spark.implicits._

    val deviceDailySQL =
      s"""
         |SELECT
         |device
         |FROM ${PropUtils.HIVE_TABLE_DEVICE_STAYING_DAILY}
         |WHERE plat='1'
         |AND day>='$beginDay'
         |AND day<='$endDay'
         |AND (area='$areaCode' OR city='$areaCode')
      """.stripMargin

    logger.info(
      s"""
         |------------------------------------------
         |deviceDailySQL:
         |$deviceDailySQL
         |------------------------------------------
      """.stripMargin)

    val inDF = spark.sql(deviceDailySQL)
    //      .coalesce(1000)

    // 去重
    val filterDF = inDF.map(row => row.getString(0)).distinct()
      .map(x => (x, uuid))
      .toDF("data", "uuid")

    val deviceCnt = filterDF.count()
    println(s"device mapping cnt $deviceCnt")

    if (jobCommon.hasRPC()) {
      sendMatchInfo(jobCommon.rpcHost, jobCommon.rpcPort, jobCommon.jobId, uuid, deviceCnt)
    }

    if (deviceCnt == 0.toLong) {
      saveDeviceToHive(spark, uuid, filterDF.coalesce(1), jobCommon)
    } else {
      saveDeviceToHive(spark, uuid, filterDF, jobCommon)
    }

  }

  def saveDeviceToHive(sparkSession: SparkSession, outputUuid: String, dataFrame: DataFrame,
    jobCommon: JobCommon): Unit = {
    // 写入hive
    val output = new JobOutput(outputUuid, "")
    DeviceCacheWriter.insertTable2(sparkSession, jobCommon, output, dataFrame,
      s"${JobName.getId(jobCommon.jobName)}|${DeviceType.DEVICE.id}")
  }

  private def sendMatchInfo(host: String, port: Int, jobId: String, uuid: String, deviceCnt: Long): Unit = {
    val outCnt = new OutCnt
    outCnt.set("device_cnt", deviceCnt)
    val matchInfo = MatchInfo(jobId, uuid, 0, deviceCnt, outCnt)
    RpcClient.send(host, port, s"2\u0001${uuid}\u0002${matchInfo.toJsonString()}")
  }
}
