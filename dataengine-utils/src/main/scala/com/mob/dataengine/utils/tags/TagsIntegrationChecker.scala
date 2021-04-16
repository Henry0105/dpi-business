package com.mob.dataengine.utils.tags

import java.util.Properties

import com.mob.dataengine.commons.utils.{AppUtils, PropUtils}
import com.mob.dataengine.utils.DateUtils
import com.mob.dataengine.utils.tags.profile.TagsGeneratorHelper
import TagsGeneratorHelper.sql
import com.mob.dataengine.utils.hbase.{HbaseChecker, Param => HParam}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import TagsGeneratorHelper._
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


case class ParamIntegration(
  tableName: String = "",
  zk: String = "",
  familyName: String = "c",
  rowKeyColName: String = "device",
  rpcHost: String = "",
  rpcPort: Int = 0,
  day: String = DateUtils.currentDay(),
  statues: String = "",
  hdfsOutput: String = "",
  sampleNum: Int = 10
) extends Serializable


case class profileInfoTmp(profileId: Int, versionId: Int, outputCode: String,
  profileName: String, plat: String
)

case class profileInfo(profileId: Int, parentId: Int, versionId: Int, profileName: String,
  plat: String, outputCode: String
) {
  val fullId: String = s"${profileId}_$versionId"

  val platInt: Int = {
    this.plat.toLowerCase() match {
      case "android" => 1
      case "ios" => 2
    }
  }

  override def toString: String = s"$fullId: parentId: $parentId, profileName: $profileName," +
    s" plat: $platInt, outputCode: $outputCode"
}


case class TagsIntegrationChecker(spark: SparkSession, hparam: ParamIntegration)
  extends HbaseChecker(spark, HParam(hparam.tableName, hparam.zk, hparam.familyName,
    hparam.rowKeyColName, hparam.rpcHost, hparam.rpcPort)) {
  val logger: Logger = LoggerFactory.getLogger(getClass)


  def run(): Unit = {

    val ip: String = AppUtils.TAG_MYSQL_JDBC_IP
    val port: Int = AppUtils.TAG_MYSQL_JDBC_PORT
    val user: String = AppUtils.TAG_MYSQL_JDBC_USER
    val pwd: String = AppUtils.TAG_MYSQL_JDBC_PWD
    val db: String = AppUtils.TAG_MYSQL_JDBC_DB
    val url = s"jdbc:mysql://$ip:$port/$db?useUnicode=true&amp;characterEncoding=UTF-8?autoReconnect=true"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("driver", "com.mysql.jdbc.Driver")

    Seq(labelOutput, profileMetadata, labelOutputProfile, profileVersion, profileCategory).foreach(tb =>
      spark.read.jdbc(url, tb, properties).createOrReplaceTempView(tb)
    )

    val profileIdWithCode: Seq[profileInfoTmp] = getToProcessProfileIds(spark)
    val profileIds = profileIdWithCode.map(_.profileId).distinct
//    logger.info(s"now print profileids count ${profileIds.size}")
//    logger.info(s"now print profileIdWithCode $profileIdWithCode")
    if (profileIds.nonEmpty) {
      val checkProfileIdAndParentId: Map[Int, Int] = getCheckProfileIdAndParentId(spark, profileIds)

      val profileInfoFull: Seq[profileInfo] = profileIdWithCode.map{ profileTmp =>
        profileInfo(profileTmp.profileId, checkProfileIdAndParentId(profileTmp.profileId),
          profileTmp.versionId, profileTmp.profileName, profileTmp.plat, profileTmp.outputCode
        )
      }

      println(s"print profileids list: ")
      profileInfoFull.foreach(println)

      val filteredDF: DataFrame = getTagInfoSample(spark, profileInfoFull, hparam.day, hparam.sampleNum)

      if (filteredDF.count() > 0) {
         checkDF(spark, filteredDF, profileInfoFull)

        val finalDF = putHDFS(spark, filteredDF, profileInfoFull, hparam.sampleNum)
        logger.info(s"now print hdfs output ${hparam.hdfsOutput}")
        val finalDFAddPar = finalDF.withColumn("_task_id", finalDF("task_id"))
        finalDFAddPar.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .partitionBy("_task_id")
          .format("json")
          .save(hparam.hdfsOutput)
        filteredDF.unpersist()
      }
    }
  }

  /*
   * return map consists: profile_id, output_code
   */
  def getToProcessProfileIds(spark: SparkSession): Seq[profileInfoTmp] = {
    import spark.implicits._
    logger.info("now get processing profile ids and show")
    sql(spark,
      s"""
         |SELECT c.profile_id, c.profile_version_id, a.output_code, d.profile_name, d.os as plat
         |FROM (
         |    SELECT id, output_code
         |    FROM $labelOutput
         |    WHERE status in (${hparam.statues})
         |) a
         |JOIN $labelOutputProfile b
         |ON a.id = b.output_id
         |JOIN $profileVersion c
         |ON b.profile_version_id = c.id
         |JOIN $profileMetadata d
         |ON c.profile_id = d.profile_id
         |GROUP BY c.profile_id, c.profile_version_id, a.output_code, d.profile_name, d.os
      """.stripMargin).map(row =>
      (row.getAs[Int]("profile_id"), row.getAs[Int]("profile_version_id"),
        row.getAs[String]("output_code"), row.getAs[String]("profile_name"),
        row.getAs[String]("plat")
      )
    ).collect().map(item => profileInfoTmp(item._1, item._2, item._3, item._4, item._5))
  }


  /*
   * return MAP consists: profile_id, profile_category_parent_id
   */
  def getCheckProfileIdAndParentId(spark: SparkSession, profileIds: Seq[Int]): Map[Int, Int] = {
    val profileId2ParentIdMapping: Map[Int, (Int, Int)] = TagsGeneratorHelper.getCategoryId2ParentIdMap(spark)
    val profileId2CategoryIdMap: Map[Int, Int] = TagsGeneratorHelper.getCategoryIdByProfileIdsV2(spark, profileIds)

    profileId2CategoryIdMap.mapValues(profileId2ParentIdMapping(_)._1)
  }


  // (spark, filteredDF, profileInfos, sampleNum)
  def putHDFS(spark: SparkSession, filteredDF: DataFrame,
    profileInfoFull: Seq[profileInfo], sampleNum: Int): DataFrame = {
      // SCHEMA ("device", "tags", "grouped_tags", "profile_id", "version_id")
//      val filteredDF = getTagInfoSample(spark, profileInfoFull: Seq[profileInfo], sampleNum)
      val fullSampleTable = "full_sample_df"
    filteredDF.createOrReplaceTempView(fullSampleTable)

    val profileInfoBC = spark.sparkContext.broadcast(
      profileInfoFull.map(p => (p.fullId, p)).toMap)

    logger.info(s"now print profile info full $profileInfoFull")
    spark.udf.register("get_profile_info", (fullId: String) => {
      val info = profileInfoBC.value(fullId)
      Map[String, String](
        "profile_id" -> info.profileId.toString,
        "version_id" -> info.versionId.toString,
        "task_id" -> info.outputCode,
        "profile_name" -> info.profileName,
        "plat" -> info.platInt.toString,
        "hbase_field" -> s"cateid_${info.parentId}"
      )
    })

    sql(spark,
      s"""
         |select
         |  cast(info['profile_id'] as int) as profile_id,
         |  info['task_id'] as task_id,
         |  cast(info['version_id'] as int) as version_id,
         |  info['profile_name'] as profile_name,
         |  cast(info['plat'] as int) as plat,
         |  info['hbase_field'] as hbase_field,
         |  data
         |from (
         |  select get_profile_info(full_id) info,
         |    collect_list(map("device",device,"value",grouped_tag)) AS data
         |  from $fullSampleTable
         |  group by full_id
         |) as a
      """.stripMargin
    )
  }


  // (spark, filteredDF, profileInfo, sampleNum)
  def checkDF(spark: SparkSession, fullSampleDF: DataFrame, profileInfoFull: Seq[profileInfo]): Unit = {
    // schema ("device", "tags", "grouped_tags", "profile_id", "version_id")
    val fullSampleTable = "full_sample_df"
    fullSampleDF.createOrReplaceTempView(fullSampleTable)

    profileInfoFull.map(_.parentId).distinct.foreach(pid => {
      val checkDFSample = sql(spark,
        s"""
           |SELECT device,
           |grouped_tag as cateid_$pid
           |FROM $fullSampleTable
           |where pid = $pid
         """.stripMargin)
      check(checkDFSample)
    })
  }

  def getTagInfoSample(spark: SparkSession, profileInfo: Seq[profileInfo], day: String, sampleNum: Int): DataFrame = {
    import spark.implicits._
    val dataset = sql(spark,
      s"""
         |SELECT device, grouped_tags, tags
         |FROM ${PropUtils.HIVE_TABLE_DM_TAGS_INFO} a
         |WHERE day = '$day' and grouped_tags is not null and size(grouped_tags) > 0
       """.stripMargin
    )

    val colList: List[String] = List("device", "grouped_tag", "full_id", "pid")

    val profileInfoBC = spark.sparkContext.broadcast(profileInfo)

    dataset.mapPartitions(it => {
      val cntArrayMap = scala.collection.mutable.Map.empty[String,
        ArrayBuffer[(String, String, String, Int)]]

      breakable{
        it.foreach(row => {
          for (profile <- profileInfoBC.value) {
            val fullId = profile.fullId
            val groupTagValue = row.getAs[Map[String, String]]("grouped_tags")
              .getOrElse(profile.parentId.toString, "")
            val tagsValue = row.getAs[Map[String, String]]("tags")
              .getOrElse(fullId, "")

            if (StringUtils.isNotBlank(groupTagValue) && StringUtils.isNoneBlank(tagsValue)) {
              if (cntArrayMap.values.isEmpty ||
                cntArrayMap.values.count(_.size < sampleNum) < profileInfoBC.value.size) {
                val element = Tuple4(
                  row.getAs[String]("device"),
                  groupTagValue,
                  fullId,
                  profile.parentId
                )

                if (!cntArrayMap.contains(fullId)) {
                  cntArrayMap(fullId) = ArrayBuffer(element)
                } else if (cntArrayMap.contains(fullId) && cntArrayMap(fullId).size < sampleNum) {
                  cntArrayMap(fullId) += element
                }
              } else {
                break
              }
            }
          }
        })
      }

      cntArrayMap.values.flatten.iterator
    }).toDF(colList: _*)
      .repartition(profileInfo.size)
      .mapPartitions{iter =>
        iter.slice(0, sampleNum)
          .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getInt(3)))
      }.toDF(colList: _*)
      .cache()
  }
}


object TagsIntegrationChecker {
  lazy private[this] val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val defaultParam: ParamIntegration = ParamIntegration()

    val projectName = s"TagsIntegrationChecker[${DateUtils.currentDay()}]"
    val parser = new OptionParser[ParamIntegration](projectName) {
      head(s"$projectName")
      opt[String]('t', "tableName")
        .text(s"hbase table name")
        .required()
        .action((x, c) => c.copy(tableName = x))
      opt[String]('z', "zk")
        .text(s"hbase zk集群组 ';'分割集群, 例如:zk1_1:2181,zk1_2:2181;zk2_1:2181,zk2_2:2181")
        .required()
        .action((x, c) => c.copy(zk = x))
      opt[Int]('p', "rpcPort")
        .text(s"thrift rpc port")
        .action((x, c) => c.copy(rpcPort = x))
      opt[String]('r', "rpcHost")
        .text(s"thrift rpc host")
        .action((x, c) => c.copy(rpcHost = x))
      opt[String]('d', "day")
        .text("day 例如:20180806 默认当天")
        .action((x, c) => c.copy(day = x))
      opt[String]('h', "hdfsOutput")
        .text("hdfs 输出路径")
        .action((x, c) => c.copy(hdfsOutput = x))
      opt[String]('o', "statues")
        .text("需要进行check的状态")
        .action((x, c) => c.copy(statues = x))
      opt[Int]('s', "sampleNum")
        .text("采样数目")
        .action((x, c) => c.copy(sampleNum = x))
    }

    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName(s"TagsIntegrationChecker[${DateUtils.currentDay()}]")
      .enableHiveSupport()
      .getOrCreate()

    parser.parse(args, defaultParam) match {
      case Some(param) =>
        logger.info(param.toString)
        TagsIntegrationChecker(spark, param).run()
      case _ => sys.exit(1)
    }
  }
}
