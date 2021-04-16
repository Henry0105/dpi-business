package com.mob.dataengine.utils.tags.fulltags.profile

import com.mob.dataengine.utils.tags.fulltags.enums.UpdateType
import org.apache.commons.lang3.{RandomStringUtils, StringUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object FullTagsGeneratorHelper {
  val kvSep = "\u0001"
  val pairSep = "\u0002"
  val pSep = "\u0003"
  /** 单体标签mysql表 */
  val individualProfile = "t_individual_profile"
  /** 标签元信息表 */
  val profileMetadata = "t_profile_metadata"
  /** 标签分类表 */
  val profileCategory = "t_profile_category"
  /** 置信度表 */
  val profileConfidence = "t_profile_confidence"
  /** 输出标签表 */
  val labelOutput = "label_output"
  /** 输出标签画像表 */
  val labelOutputProfile = "label_output_profile"
  /** 标签版本表 */
  val profileVersion = "t_profile_version"
  // 中间件存储使用hive分区表的信息表
  val midengineStatus = "t_midengine_status"

  def sql(spark: SparkSession, query: String): DataFrame = {
    println(
      s"""
         |<<<<<<
         |$query
         |>>>>>>
        """.stripMargin)
    spark.sql(query)
  }

  def buildHiveTableFromProfiles(spark: SparkSession, sample: Boolean, profiles: Array[ProfileInfo],
                                 tbManager: TablePartitionsManager, day: String): HiveTable = {
    val profile = profiles.head
    val key = profile.key(spark)
    val partitionClause = if (profile.isPartitionTable) {
      val part = getAssignPartition(spark, profile, day, tbManager)
      if (StringUtils.isNotBlank(part)) part else " false "
    } else {
      " true "
    }

    val updateTimeClause = getUpdateTime(spark, profile, day)

    val whereClause =
      s"""
         |where (${profiles.map(p => s"(${p.whereClause()})").toSet.mkString(" or ")})
         |  and $updateTimeClause
         |  and $partitionClause
         |  and $key rlike '[0-9a-f]{40}' and ${sampleClause(sample)}
         """.stripMargin

    HiveTable(profile.profileDatabase, profile.profileTable, key,
      profile.isPartitionTable, whereClause, partitionClause)
  }

  // 2种情况,全量表需要取update_time(目前都是日更新),增量表直接走分区筛选
  def getUpdateTime(spark: SparkSession, profileInfo: ProfileInfo, day: String): String = {
    if (UpdateType.isFull(profileInfo.updateType)) {
      profileInfo.profileTable match {
        case "rp_device_profile_full_history_view" => s" processtime_all <= '$day' "
        case _ => s" processtime <= '$day' "
      }
    } else {
      " true "
    }
  }

  def valueToStr(dataType: String, value: String): String = {
    dataType match {
      case "string" => s"$value"
      case "double" => s"num2str($value)"
      case _ => s"cast($value as string)"
    }
  }

  // concat(kvSep, gender, 1), concat(kvSep, agebin, 3)
  // concat(1_1000, kvSep, cast(gender_cl as string),concat(2_1000, kvSep, cast(agebin_cl as string)
  def buildMapStringFromFields[T <: ProfileData](arr: Array[T], kvSep: String): String = {
    arr.map(_.columnClause(kvSep)).mkString(",")
  }

  // 处理taglist这样字段的udf
  def processTaglistLikeFields(spark: SparkSession, arr: Array[ProfileInfo], field: String): String = {
    val m = arr.map(p => p.profileColumn.split(";")(1) -> s"${p.fullVersionId}").toMap
    val mBC = spark.sparkContext.broadcast(m)
    val fn = s"process_${field}_${RandomStringUtils.randomAlphanumeric(5)}"

    spark.udf.register(s"$fn", (f: String) => {
      val tmp = f.split("=")
      if (tmp.length > 1) {
        val pairs = tmp(0).split(",").zip(tmp(1).split(",")).flatMap { case (tagId, v) =>
          mBC.value.get(tagId).map(fullId => s"$fullId$kvSep$v")
        }
        if (pairs.isEmpty) {
          null
        } else {
          pairs.mkString(pairSep)
        }
      } else {
        null
      }
    })

    s"$fn($field)"
  }

  def sampleClause(sample: Boolean, key: String = "device"): String = {
    val randomType = (Math.random() * 100000).toInt
    if (sample) {
      s"hash($key)%1000000=$randomType"
    } else {
      "true"
    }
  }

  def getAssignPartition(spark: SparkSession, profileInfo: ProfileInfo, day: String,
                         tbManager: TablePartitionsManager): String = {
    val parts = tbManager.getPartitions(profileInfo.fullTableName)
    val assignParts = parts.foldLeft(List.empty[String])((xs, x) =>
      if ("""\d{8}""".r.findFirstMatchIn(x).get.matched == day) x :: xs else xs
    )

    val column = profileInfo.profileColumn
    lazy val fields = column.split(";")(1).split("and")
    val noParFields = Array("plat")

    if (assignParts.nonEmpty) {
      if (column.contains("flag") && column.contains("timewindow")) {
        getDayByTimewindowFlag(assignParts, profileInfo)
      } else if (column.contains(";") && fields.length == 1 &&
        !noParFields.contains(fields(0).split("=")(0).trim)) {
        getDayByOneField(assignParts, profileInfo)
      } else {
        assignParts.head
      }
    } else ""
  }

  def getDayByTimewindowFlag(parts: Seq[String], profileInfo: ProfileInfo): String = {
    val flag = """flag=(\d+)""".r.findFirstMatchIn(profileInfo.profileColumn).get.subgroups.head
    val timeWindow = """timewindow='(\d+)'""".r.findFirstMatchIn(profileInfo.profileColumn).get.subgroups.head
    println(s"filter ${profileInfo.fullTableName} with flag=$flag and timewindow=$timeWindow")
    val fdt = parts.filter { p =>
      val arr = p.split("/")
      arr.contains(s"flag=$flag") && arr.contains(s"timewindow=$timeWindow")
    }
    if (fdt.nonEmpty) {
      /** 对于timewindow 需要只取出day字段 */
      """day=(\d+)""".r.findFirstMatchIn(fdt.last).get.matched
    } else {
      ""
    }
  }

  def getDayByOneField(parts: Seq[String], profileInfo: ProfileInfo): String = {
    val fieldKey = profileInfo.profileColumn.split(";")(1).split("=")(0).trim
    val fieldValue = s"""$fieldKey='(.+)'""".r.findFirstMatchIn(profileInfo.profileColumn).get.subgroups.head
    println(s"filter ${profileInfo.fullTableName} with $fieldKey=$fieldValue")
    val fdt = parts.filter(p => p.contains(s"$fieldKey=$fieldValue"))
    if (fdt.nonEmpty) {
      """day=(\d+)""".r.findFirstMatchIn(fdt.last).get.matched
    } else {
      ""
    }
  }

  /** 必须是timewindow的表 */
  def getValue2IdMapping(profiles: Array[ProfileInfo]): Map[String, String] = {
    val tmp = ArrayBuffer[(String, String)] ()
    profiles.flatMap(p => {
      if (p.profileColumn.contains("feature in")) {
        val value: String = """feature in \((\S+)\)""".r.findFirstMatchIn(p.profileColumn).get.subgroups.head
        value.replace("'", "").split(",").map(_ -> p.fullVersionId)
      } else if (p.profileColumn.contains("feature=")) {
        val value = """feature='([^']+)'""".r.findFirstMatchIn(p.profileColumn).get.subgroups.head
        Array(value -> p.fullVersionId)
      } else Array.empty[(String, String)]
    })
    tmp.toMap
  }


  def getProfilesFilterPartition(spark: SparkSession, profileIdFullTable: String): Array[ProfileInfo] = {
    import spark.implicits._
    sql(spark,
      s"""
         |select a.profile_id,
         |       profile_version_id,
         |       (case when profile_table = 'rp_device_profile_full_view'
         |       then 'rp_mobdi_app' else profile_database end) as profile_database,
         |       (case when profile_table = 'rp_device_profile_full_view'
         |       then 'rp_device_profile_full_history_view' else profile_table end) as profile_table,
         |       profile_column,
         |       (case when profile_table like '%view%' then 0 else has_date_partition end) as has_date_partition,
         |       profile_datatype,
         |       update_type,
         |       partition_type
         |from $individualProfile as a
         |inner join $profileIdFullTable as b
         |on concat(a.profile_id, '_', a.profile_version_id) = b.profile_id
       """.stripMargin)
      .map(r => ProfileInfo(
        r.getAs[Int]("profile_id"),
        r.getAs[Int]("profile_version_id"),
        r.getAs[String]("profile_database"),
        r.getAs[String]("profile_table"),
        r.getAs[String]("profile_column"),
        r.getAs[Int]("has_date_partition"),
        r.getAs[String]("profile_datatype"),
        r.getAs[String]("update_type"),
        r.getAs[Int]("partition_type")
      )).collect()
  }

  def customUpdateStr2Map(mapString: String, pairSep: String, kvSep: String): Map[String, Array[String]] = {
    if (StringUtils.isBlank(mapString)) {
      null
    } else {
      val tmp = mapString.split(pairSep, -1).flatMap { e =>
        val arr = e.split(kvSep, 3)
        if (arr.length == 3 && StringUtils.isNotBlank(arr(1))) {
          Some(arr(0), Array(arr(1), arr(2)))
        } else {
          None
        }
      }.toMap[String, Array[String]]
      if (tmp.isEmpty) {
        null
      } else {
        tmp
      }
    }
  }

  def cleanConfidence2mapArr(confMap: Map[String, String], valueMap: Map[String, Seq[String]],
                             day: String): Map[String, Array[String]] = {
    if (null == confMap || confMap.size < 1 || null == valueMap || valueMap.size < 1) {
      null
    } else {
      // 如果valueMap中没有这个key也去掉该记录
      val tmp = confMap.filterKeys{ k =>
        valueMap.get(k) match {
          case Some(null) => false
          case Some(x) if StringUtils.isBlank(x.head) || x.head=="-1" => false
          case None => false
          case _ => true
        }
      }
      if (tmp.isEmpty) {
        null
      } else {
        tmp.mapValues(v => Array(v, day))
      }
    }
  }

  def insertUpdateTime(kv: String, updateTime: String): String = {
    if (StringUtils.isBlank(updateTime)) kv
    else kv.split(pairSep).map(str => s"$str$kvSep$updateTime").mkString(pairSep)
  }

  def getConfidenceProfileIds(spark: SparkSession, confidenceTable: String): Array[(Int, Int)] = {
    import spark.implicits._
    sql(spark,
      s"""
         |select profile_id, profile_version_id
         |from $confidenceTable
       """.stripMargin)
      .map(r => (r.getAs[Int]("profile_id"), r.getAs[Int]("profile_version_id")))
      .collect()
  }
}

case class TablePartitionsManager(spark: SparkSession) {
  val tablePartitions: mutable.Map[String, Seq[String]] = mutable.Map.empty[String, Seq[String]]

  def getPartitions(tableName: String): Seq[String] = {
    if (tablePartitions.contains(tableName)) {
      tablePartitions(tableName)
    } else {
      import spark.implicits._
      val parts = FullTagsGeneratorHelper.sql(spark, s"show partitions $tableName").map(_.getString(0)).collect()
      tablePartitions.put(tableName, parts)
      parts
    }
  }
}
