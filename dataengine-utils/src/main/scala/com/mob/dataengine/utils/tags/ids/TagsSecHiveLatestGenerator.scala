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
  * pid/ieid/mcid/device
  * 创建四张tag表
  *
  * @author wangteng
  */

case class ParamsSec(
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

object TagsSecHiveLatestGenerator {
  def main(args: Array[String]): Unit = {
    val defaultParams: ParamsSec = ParamsSec()
    val projectName = s"TagsHiveLatestGenerator[${DateUtils.currentDay()}]"

    val parser = new OptionParser[ParamsSec](projectName) {
      opt[String]('i', "id_types")
        .text("device|mcid|ieid|pid")
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
          Seq("pid", "ieid", "mcid", "device")
        } else {
          params.idTypes.split(",").toSeq
        }
        new TagsSecHiveLatestGenerator(params.day, spark).generateTags(tableType)
        spark.stop()
    }

  }
}

class TagsSecHiveLatestGenerator(version: String, @transient spark: SparkSession) extends Serializable {
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

    val lastPar = sql(
      s"SHOW PARTITIONS ${PropUtils.HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE}"
    ).collect.map(_.getString(0)).toList.max.split("=")(1)

    val profileFullDF = spark.table(PropUtils.HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE
    ).filter($"version" === lastPar).drop(
      "version"
    )
    if (tableTypes.contains("pid")) {
      generatePidTags(profileFullDF)
    }
    if (tableTypes.contains("device")) {
      generateDeviceTags(profileFullDF)
    }
    if (tableTypes.contains("ieid")) {
      generateIeidTags(profileFullDF)
    }
    if (tableTypes.contains("mcid")) {
      generateMcidTags(profileFullDF)
    }

  }

  def insertOverwriteToHiveTable(sourceTableName: String, targetTableName: String): Unit = {
    sql(
      s"""
         |INSERT OVERWRITE TABLE
         |    $targetTableName partition (version=$version)
         |  SELECT
         |    device,
         |    mcid,
         |    ieid,
         |    pid,
         |    ${tagFields.filterNot(_.equals("device")).filterNot(_.equals("location_day")).mkString(",")},
         |    match_flag
         |  FROM $sourceTableName
      """.stripMargin)
  }

  def getLastPar(table: String): String = {
    val lastPar = sql(
      s"SHOW PARTITIONS $table"
    ).collect.map(_.getString(0)).toList.max.split("/")(0).split("=")(1)
    logger.info(s"partition is $lastPar")
    lastPar
  }

  def generateIeidTags(profileFullDF: DataFrame): Unit = {
    val lastPar = getLastPar(PropUtils.HIVE_TABLE_DM_IEID_MAPPING)

    val originalMappingDF = spark.table(
      PropUtils.HIVE_TABLE_DM_IEID_MAPPING
    ).where($"day" === lastPar).filter(length(trim($"ieid")) > 0 and size($"device") > 0
    ).withColumn(
      "ieid", lower(trim($"ieid"))
    ).withColumn(
      "device", expr("latest(device,device_ltm)")
    ).withColumn(
      "mcid", regexp_replace(expr("latest(mcid,mcid_ltm)"), ":|-", "")
    ).withColumn(
      "pid", expr("latest(pid,pid_ltm)")
    ).select("device", "ieid", "mcid", "pid")

    val tableName = "tmp_ieid_mapping"

    createView(originalMappingDF, profileFullDF, tableName)
    insertOverwriteToHiveTable(tableName, PropUtils.HIVE_TABLE_DM_IEID_LATEST_TAGS_MAPPING)
    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DM_IEID_LATEST_TAGS_MAPPING, version)
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

  def generateMcidTags(profileFullDF: DataFrame): Unit = {
    val lastPar = getLastPar(PropUtils.HIVE_TABLE_DM_MCID_MAPPING)

    val originalMappingDF = spark.table(
      PropUtils.HIVE_TABLE_DM_MCID_MAPPING
    ).where($"day" === lastPar).filter(length(trim($"mcid")) > 0 and size($"device") > 0
    ).withColumn(
      "mcid", lower(regexp_replace(trim($"mcid"), ":|-", ""))
    ).withColumn(
      "device", expr("latest(device,device_ltm)")
    ).withColumn(
      "ieid", expr("latest(ieid,ieid_ltm)")
    ).withColumn(
      "pid", expr("latest(pid,pid_ltm)")
    ).select("device", "mcid", "ieid", "pid")

    val tableName = "tmp_mcid_mapping"
    createView(originalMappingDF, profileFullDF, tableName)
    insertOverwriteToHiveTable(tableName, PropUtils.HIVE_TABLE_DM_MCID_LATEST_TAGS_MAPPING)
    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DM_MCID_LATEST_TAGS_MAPPING, version)
  }

  def generatePidTags(profileFullDF: DataFrame): Unit = {
    val lastPar = getLastPar(PropUtils.HIVE_TABLE_DM_PID_MAPPING)

    val originalMappingDF = spark.table(
      PropUtils.HIVE_TABLE_DM_PID_MAPPING
    ).where($"day" === lastPar).filter(length(trim($"pid")) > 0 and size($"device") > 0
    ).withColumn(
      "pid", trim($"pid")
    ).withColumn(
      "device", expr("latest(device,device_ltm)")
    ).withColumn(
      "ieid", expr("latest(ieid,ieid_ltm)")
    ).withColumn(
      "mcid", regexp_replace(expr("latest(mcid,mcid_ltm)"), ":|-", "")
    ).select("device", "mcid", "ieid", "pid")

    val tableName = "tmp_pid_mapping"
    createView(originalMappingDF, profileFullDF, tableName)
    insertOverwriteToHiveTable(tableName, PropUtils.HIVE_TABLE_DM_PID_LATEST_TAGS_MAPPING)
    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DM_PID_LATEST_TAGS_MAPPING, version)
  }

  def generateDeviceTags(profileFullDF: DataFrame): Unit = {
    val lastPar = getLastPar(PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC)

    val originalMappingDF = spark.table(
      PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC
    ).where($"day" === lastPar).filter($"device".isNotNull
    ).withColumn(
      "device", lower(trim($"device"))
    ).withColumn(
      "pid", expr("latest(pid,pid_ltm)")
    ).withColumn(
      "ieid", expr("latest(ieid,ieid_ltm)")
    ).withColumn(
      "mcid", regexp_replace(expr("latest(mcid,mcid_ltm)"), ":|-", "")
    ).select("device", "mcid", "ieid", "pid")

    val tableName = "tmp_device_mapping"
    createView(originalMappingDF, profileFullDF, tableName)

    insertOverwriteToHiveTable(tableName, PropUtils.HIVE_TABLE_DM_DEVICE_SEC_LATEST_TAGS_MAPPING)
    SparkUtils.createView(spark, PropUtils.HIVE_TABLE_DM_DEVICE_SEC_LATEST_TAGS_MAPPING, version)
  }
}
