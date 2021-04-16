package com.mob.dataengine.utils.tags.ids

import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import com.mob.dataengine.rpc.RpcClient
import com.mob.dataengine.utils.SparkUtils
import com.mob.dataengine.utils.geohash.GeoHashUDF
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scopt.OptionParser


/*
  * enhance/phone/imei/mac/device
  * 创建四张tag表
  *
  * @author wangteng
  */

case class Params(
  day: String = DateUtils.currentDay(),
  idTypes: String = "",
  rpcHost: Option[String] = None,
  rpcPort: Option[Int] = None,
  sample: Boolean = false
) extends Serializable {
  def isRpc: Boolean = {
    rpcHost.isDefined && rpcPort.isDefined
  }
}

object TagsHiveLatestGenerator {
  def main(args: Array[String]): Unit = {
    val defaultParams: Params = Params()
    val projectName = s"TagsHiveLatestGenerator[${DateUtils.currentDay()}]"

    val parser = new OptionParser[Params](projectName) {
      opt[String]('i', "id_types")
        .text("device|mac|imei|phone|enhance")
        .required()
        .action((x, c) => c.copy(idTypes = x))
      opt[String]('d', "day")
        .text("day 例如:20180806 默认当天")
        .action((x, c) => c.copy(day = x))
      opt[Int]('p', "rpcPort")
        .text(s"thrift rpc port")
        .action((x, c) => c.copy(rpcPort = Some(x)))
      opt[String]('h', "rpcHost")
        .text(s"thrift rpc host")
        .action((x, c) => c.copy(rpcHost = Some(x)))
      opt[Boolean]('s', "sample")
        .text(s"sample")
        .action((x, c) => c.copy(sample = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        val spark = SparkSession.builder()
          .appName(projectName)
          .config("spark.sql.orc.impl", "hive")
          .config("spark.sql.orc.enableVectorizedReader", "true")
          .enableHiveSupport()
          .getOrCreate()
        if (params.isRpc) {
          RpcClient.send(params.rpcHost.get, params.rpcPort.get, s"1\u0001${spark.sparkContext.applicationId}")
        }

        val tableType = if (args == null || args.isEmpty) {
          Seq("enhance", "phone", "imei", "mac", "device")
        } else {
          params.idTypes.split(",").toSeq
        }
        new TagsHiveLatestGenerator(params.day, spark).generateTags(tableType)
        spark.stop()
    }

  }
}

class TagsHiveLatestGenerator(version: String, @transient spark: SparkSession) extends Serializable {
  @transient private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private val profileFullFields = Seq(
    "device",
    "country",
    "province",
    "city",
    "gender",
    "agebin",
    "segment",
    "edu",
    "kids",
    "income",
    "cell_factory",
    "model",
    "model_level",
    "carrier",
    "network",
    "screensize",
    "sysver",
    "tot_install_apps",
    "country_cn",
    "province_cn",
    "city_cn",
    "city_level",
    "permanent_country",
    "permanent_province",
    "permanent_city",
    "occupation",
    "house",
    "repayment",
    "car",
    "workplace",
    "residence",
    "married",
    "applist",
    "permanent_country_cn",
    "permanent_province_cn",
    "permanent_city_cn",
    "public_date",
    "consum_level",
    "life_stage",
    "industry",
    "identity",
    "special_time",
    "processtime",
    "agebin_1001",
    "nationality",
    "nationality_cn",
    "breaked",
    "last_active",
    "group_list",
    "city_level_1001",
    "first_active_time",
    "price"
  )

  val tagFields: Seq[String] = profileFullFields ++ Seq(
    "catelist",
    "tag_list",
    "frequency_geohash_list",
    "home_geohash",
    "work_geohash",
    "location_day"
  )

  def sql(sqlString: String): DataFrame = {
    logger.info(
      s"""
         |---------------------------------------------------------------
         |$sqlString
         |---------------------------------------------------------------
       """.stripMargin
    )
    spark.sql(sqlString)
  }

  def getGeohash7(row: Row): String = {
    if (null == row || null == row.getAs[Any]("lat") || null == row.getAs[Any]("lon")) {
      null
    } else {
      GeoHashUDF.instance.getGeoHashBase32ByLv(7)(
        row.getAs[String]("lat").toDouble, row.getAs[String]("lon").toDouble
      )
    }
  }

  def prepare(): Unit = {
    // return latest ele in arr according to tm seq
    spark.udf.register("latest",
      (arr: Seq[String], tm: Seq[String]) => {
        if (arr != null && arr.nonEmpty) {
          tm.zip(arr).sortWith((a, b) => a._1 > b._1).head._2
        } else {
          null
        }
      }
    )
    spark.udf.register("geohash7", (r: Row) => getGeohash7(r))
    spark.udf.register("geohash7s",
      (rows: Seq[Row]) => {
        rows.filter(r => r != null).map(r => getGeohash7(r)).filter(gh => gh != null).mkString(",")
      }
    )
  }

  def generateTags(tableTypes: Seq[String]): Unit = {
    prepare()
    if (tableTypes.contains("enhance")) {
      enhanceProfileFull()
    }

    val lastPar = sql(
      s"SHOW PARTITIONS ${PropUtils.HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE}"
    ).collect.map(_.getString(0)).toList.max.split("=")(1)

    val profileFullDF = spark.table(PropUtils.HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE
    ).filter($"version" === lastPar).drop(
      "version"
    )
    if (tableTypes.contains("phone")) {
      generatePhoneTags(profileFullDF)
    }
    if (tableTypes.contains("device")) {
      generateDeviceTags(profileFullDF)
    }
    if (tableTypes.contains("imei")) {
      generateImeiTags(profileFullDF)
    }
    if (tableTypes.contains("mac")) {
      generateMacTags(profileFullDF)
    }

  }

  def insertOverwriteToHiveTable(sourceTableName: String, targetTableName: String): Unit = {
    sql(
      s"""
         |INSERT OVERWRITE TABLE
         |    $targetTableName partition (version=$version)
         |  SELECT
         |    device,
         |    mac,
         |    imei,
         |    phone,
         |    ${tagFields.filterNot(_.equals("device")).filterNot(_.equals("location_day")).mkString(",")},
         |    match_flag
         |  FROM $sourceTableName
      """.stripMargin)
  }

  def enhanceProfileFull(): Unit = {
    val lastPartitionHomeWork = spark.sql(
      s"show partitions ${PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK}")
      .collect.toList.last.getString(0).split("=")(1)
    val lastPartitionFreq = spark.sql(s"show partitions ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}")
      .collect.toList.last.getString(0).split("=")(1)
    val lastPar = if (lastPartitionHomeWork > lastPartitionFreq) lastPartitionHomeWork else lastPartitionFreq
    val geohash7DF = sql(
      s"""
         |SELECT
         |  device,
         |  CASE WHEN MAX(frequency) IS NOT NULL THEN geohash7s(MAX(frequency)) ELSE NULL END AS frequency_geohash_list,
         |  CASE WHEN MAX(home) IS NOT NULL THEN geohash7(MAX(home)) ELSE NULL END AS home_geohash,
         |  CASE WHEN MAX(work) IS NOT NULL THEN geohash7(MAX(work)) ELSE NULL END AS work_geohash,
         |  $lastPar as location_day
         | FROM (
         |     SELECT device
         |             ,named_struct(
         |                 'lat', CAST(lat_home AS string),
         |                 'lon', CAST(lon_home AS string)
         |             ) AS home
         |             ,named_struct(
         |                 'lat', CAST(lat_work AS string),
         |                 'lon', CAST(lon_work AS string)
         |              ) AS work
         |             ,NULL AS frequency
         |         FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK}
         |         WHERE day = '$lastPartitionHomeWork'
         |
         |         UNION ALL
         |         SELECT device
         |             ,NULL AS home
         |             ,NULL AS work
         |             ,COLLECT_LIST(
         |                 named_struct(
         |                     'lat', CAST(lat AS string),
         |                     'lon', CAST(lon AS string)
         |                 )
         |             ) AS frequency
         |         FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY}
         |         WHERE day = '$lastPartitionFreq'
         |         GROUP BY device
         | ) HWF
         | GROUP BY device
       """.stripMargin
    )

    val tagLastPar = sql(s"show partitions ${PropUtils.HIVE_TABLE_TAG_CAT_MAPPING_DMP_PAR}").collect()
      .map(_.getAs[String](0))
      .max
    val tagListMapping = sql(
      s"""
         |SELECT
         |tag_id,concat(cat1_id,',',cat2_id)
         |FROM ${PropUtils.HIVE_TABLE_TAG_CAT_MAPPING_DMP_PAR}
         |WHERE $tagLastPar
      """.stripMargin
    ).collect().map { x => (x.getString(0), x.getString(1)) }.toMap

    // 生成categoryMap并且广播
    val categoryMapping = sql(
      s"""
         | SELECT apppkg,cate_l2_id
         | FROM
         |  ${PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR}
         | WHERE version='1000'
         | GROUP BY apppkg,cate_l2_id
      """.stripMargin
    ).collect().map(row => row.getString(0) -> row.getString(1)).toMap

    val categoryMapBC = spark.sparkContext.broadcast(categoryMapping)
    val tagListMapBC = spark.sparkContext.broadcast(tagListMapping)

    // 针对profile_full表中device处理出category_list
    def getAppTagCat: UDF2[String, String, Row] = new UDF2[String, String, Row] {
      override def call(appList: String, tagList: String): Row = {
        val cateListStr =
          appList.split(",")
            .flatMap(x => categoryMapBC.value.getOrElse(x, "").split(",")).filter(_.length > 0)
            .distinct.mkString(",")

        val tagListStr = if (StringUtils.isNotBlank(tagList) && tagList.contains("=")) {
          tagList.split("=")(0).split(",").flatMap(x =>
            Array.concat(tagListMapBC.value.getOrElse(x, "").split(","), x.split(","))
          ).filter(_.length > 0
          ).distinct.mkString(",")
        } else {
          null
        }
        Row.apply(cateListStr, tagListStr)
      }
    }

    spark.udf.register("fun_app_tag", getAppTagCat, StructType(
      StructField("catelist", StringType, nullable = true) ::
        StructField("tag_list", StringType, nullable = true) :: Nil))

    val profileDF = spark.table(PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL).withColumn(
      "cat", expr("fun_app_tag(applist,tag_list)")
    ).drop(
      "tag_list", "catelist",
      "processtime_all", "model_flag", "stats_flag",
      "mapping_flag", "version").selectExpr(
      profileFullFields ++ Seq(
        "cat.catelist as catelist",
        "cat.tag_list as tag_list"
      ): _*
    )

    profileDF.join(geohash7DF, Seq("device"), "left_outer").selectExpr(
        tagFields: _*
    ).createOrReplaceTempView("tmp_final_enhance")

    spark.table("tmp_final_enhance").printSchema()

    sql(
      s"""
         |INSERT OVERWRITE TABLE
         |    ${PropUtils.HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE} partition (version=$version)
         |  SELECT
         |    ${tagFields.mkString(",")}
         |  FROM tmp_final_enhance
      """.stripMargin)

    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE, version)
  }

  def getLastPar(table: String): String = {
    val lastPar = sql(
      s"SHOW PARTITIONS $table"
    ).collect.map(_.getString(0)).toList.max.split("/")(0).split("=")(1)
    logger.info(s"partition is $lastPar")
    lastPar
  }

  def generateImeiTags(profileFullDF: DataFrame): Unit = {
    val lastPar = getLastPar(PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3)

    val originalMappingDF = spark.table(
      PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3
    ).where($"day" === lastPar).filter($"imei".isNotNull
    ).withColumn(
      "imei", lower(trim($"imei"))
    ).withColumn(
      "device", expr("latest(device,device_ltm)")
    ).withColumn(
      "mac", regexp_replace(expr("latest(mac,mac_ltm)"), ":|-", "")
    ).withColumn(
      "phone", expr("latest(phone,phone_ltm)")
    ).select("device", "imei", "mac", "phone")

    val tableName = "tmp_imei_mapping"

    createView(originalMappingDF, profileFullDF, tableName)
    insertOverwriteToHiveTable(tableName, PropUtils.HIVE_TABLE_DM_IMEI_LATEST_TAGS_MAPPING)
    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DM_IMEI_LATEST_TAGS_MAPPING, version)
  }

  private def createView(originalMappingDF: DataFrame, profileFullDF: DataFrame, tableName: String): Unit = {
    val resultDF = originalMappingDF.join(profileFullDF,
      originalMappingDF("device") === profileFullDF("device"),
      "left_outer"
    ).withColumn(
      "match_flag", when(profileFullDF("device").isNotNull, 1).otherwise(0)
    ).drop(profileFullDF("device"))
    resultDF.createOrReplaceTempView(tableName)
  }

  def generateMacTags(profileFullDF: DataFrame): Unit = {
    val lastPar = getLastPar(PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3)

    val originalMappingDF = spark.table(
      PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3
    ).where($"day" === lastPar).filter($"mac".isNotNull
    ).withColumn(
      "mac", lower(regexp_replace(trim($"mac"), ":|-", ""))
    ).withColumn(
      "device", expr("latest(device,device_ltm)")
    ).withColumn(
      "imei", expr("latest(imei,imei_ltm)")
    ).withColumn(
      "phone", expr("latest(phone,phone_ltm)")
    ).select("device", "mac", "imei", "phone")

    val tableName = "tmp_mac_mapping"
    createView(originalMappingDF, profileFullDF, tableName)
    insertOverwriteToHiveTable(tableName, PropUtils.HIVE_TABLE_DM_MAC_LATEST_TAGS_MAPPING)
    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DM_MAC_LATEST_TAGS_MAPPING, version)
  }

  def generatePhoneTags(profileFullDF: DataFrame): Unit = {
    val lastPar = getLastPar(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3)

    val originalMappingDF = spark.table(
      PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3
    ).where($"day" === lastPar).filter($"phone".isNotNull
    ).withColumn(
      "phone", trim($"phone")
    ).withColumn(
      "device", expr("latest(device,device_ltm)")
    ).withColumn(
      "imei", expr("latest(imei,imei_ltm)")
    ).withColumn(
      "mac", regexp_replace(expr("latest(mac,mac_ltm)"), ":|-", "")
    ).select("device", "mac", "imei", "phone")

    val tableName = "tmp_phone_mapping"
    createView(originalMappingDF, profileFullDF, tableName)
    insertOverwriteToHiveTable(tableName, PropUtils.HIVE_TABLE_DM_PHONE_LATEST_TAGS_MAPPING)
    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DM_PHONE_LATEST_TAGS_MAPPING, version)
  }

  def generateDeviceTags(profileFullDF: DataFrame): Unit = {
    val lastPar = getLastPar(PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3)

    val originalMappingDF = spark.table(
      PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3
    ).where($"day" === lastPar).filter($"device".isNotNull
    ).withColumn(
      "device", lower(trim($"device"))
    ).withColumn(
      "phone", expr("latest(phone,phone_ltm)")
    ).withColumn(
      "imei", expr("latest(imei,imei_ltm)")
    ).withColumn(
      "mac", regexp_replace(expr("latest(mac,mac_ltm)"), ":|-", "")
    ).select("device", "mac", "imei", "phone")

    val tableName = "tmp_device_mapping"
    createView(originalMappingDF, profileFullDF, tableName)

    insertOverwriteToHiveTable(tableName, PropUtils.HIVE_TABLE_DM_DEVICE_LATEST_TAGS_MAPPING)
    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DM_DEVICE_LATEST_TAGS_MAPPING, version)
  }
}
