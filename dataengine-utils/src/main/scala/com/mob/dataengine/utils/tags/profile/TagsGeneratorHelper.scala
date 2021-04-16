package com.mob.dataengine.utils.tags.profile

import org.apache.commons.lang3.{RandomStringUtils, StringUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object TagsGeneratorHelper {
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
    tbManager: TablePartitionsManager): HiveTable = {
    val profile = profiles.head
    val key = profile.key(spark)
    val partitionClause = if (profile.isPartitionTable) {
      getLatestDatePartition(spark, profile, tbManager)
    } else {
      " true "
    }
    val whereClause =
      s"""
         |where (${profiles.map(p => s"(${p.whereClause()})").toSet.mkString(" or ")})
         |  and $partitionClause
         |  and $key rlike '[0-9a-f]{40}' and ${sampleClause(sample)}
         """.stripMargin

    HiveTable(profile.profileDatabase, profile.profileTable, key,
      profile.isPartitionTable, whereClause, partitionClause)
  }

  def profileIdUpdateTime(spark: SparkSession): Map[String, String] = {
    import spark.implicits._
    sql(spark,
      s"""
         |select concat(profile_id, '_', profile_version_id) full_id, hive_partition
         |from $midengineStatus
       """.stripMargin
    ).map(r => r.getAs[String]("full_id") -> r.getAs[String]("hive_partition"))
      .collect().toMap
  }

  // concat(kvSep, gender, 1), concat(kvSep, agebin, 3)
  // concat(1_1000, kvSep, cast(gender_cl as string),concat(2_1000, kvSep, cast(agebin_cl as string)
  def buildMapStringFromFields[T <: ProfileData](arr: Array[T], kvSep: String): String = {
    arr.map(_.columnClause(kvSep)).mkString(",")
  }

  def buildMapStringFromFieldsParent[T <: ProfileData](arr: Array[T], kvSep: String,
    fullId2ParentMap: Map[String, String]): String = {
    arr.map(_.pColumnClause(kvSep, fullId2ParentMap)).mkString(",")
  }

  // 处理taglist这样字段的udf
  def processTaglistLikeFields(spark: SparkSession, arr: Array[ProfileInfo], field: String,
    fullId2PidMap: Map[String, String]): String = {
    val m = arr.map(p => p.profileColumn.split(";")(1)
      -> s"${fullId2PidMap(p.fullVersionId)}$pSep${p.fullVersionId}").toMap
    val mBC = spark.sparkContext.broadcast(m)
    val fn = s"process_${field}_${RandomStringUtils.randomAlphanumeric(5)}"

    spark.udf.register(s"$fn", (f: String) => {
      if (StringUtils.isNotBlank(f)) {
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

  def getLatestDatePartition(spark: SparkSession, profileInfo: ProfileInfo,
    tbManager: TablePartitionsManager): String = {
    val parts = tbManager.getPartitions(profileInfo.fullTableName)
    val sortedParts = parts.sortBy(p => """\d{8}""".r.findFirstMatchIn(p).get.matched)
    if (profileInfo.profileColumn.contains("flag") && profileInfo.profileColumn.contains("timewindow")) {
      val flag = """flag=(\d+)""".r.findFirstMatchIn(profileInfo.profileColumn).get.subgroups.head
      val timeWindow = """timewindow='(\d+)'""".r.findFirstMatchIn(profileInfo.profileColumn).get.subgroups.head
      println(s"filter ${profileInfo.fullTableName} with flag=$flag and timewindow=$timeWindow")
      val fdt = sortedParts.filter{p =>
        val arr = p.split("/")
        arr.contains(s"flag=$flag") && arr.contains(s"timewindow=$timeWindow")
      }.last

      /** 对于timewindow 需要只取出day字段 */
      """day=(\d+)""".r.findFirstMatchIn(fdt).get.matched
    } else if (profileInfo.profileColumn.contains("feature=")) { // v3表
      val feature = """feature='(.+)'""".r.findFirstMatchIn(profileInfo.profileColumn).get.subgroups.head
      println(s"filter ${profileInfo.fullTableName} with feature=$feature")
      val fdt = sortedParts.filter(p => p.contains(s"feature=$feature")).last
      """day=(\d+)""".r.findFirstMatchIn(fdt).get.matched
    } else {
      sortedParts.last.replaceAll("/", " and ")
    }
  }

  def getCategoryIdByProfileIds(spark: SparkSession, profileIdsTable: String): Map[Int, Int] = {
    import spark.implicits._

    sql(spark,
      s"""
         |select a.profile_id, profile_category_id
         |from $profileMetadata as a
         |inner join $profileIdsTable as b
         |on a.profile_id = b.profile_id
      """.stripMargin)
      .map(r => (r.getAs[Int]("profile_id"), r.getAs[Int]("profile_category_id")))
      .collect().toMap
  }

  def getCategoryIdByProfileIdsV2(spark: SparkSession, profileIds: Seq[Int]): Map[Int, Int] = {
    import spark.implicits._

    sql(spark,
      s"""
         |SELECT a.profile_id, profile_category_id
         |FROM $profileMetadata as a
         |WHERE a.profile_id IN (${profileIds.mkString(",")})
      """.stripMargin)
      .map(r => (r.getAs[Int]("profile_id"), r.getAs[Int]("profile_category_id")))
      .collect().toMap
  }

  /*
   * get profile infos by profileids
   * return: profile_id -> (profile_name, plat)
   */
  def getProfileInfoByProfileIds(spark: SparkSession, profileIds: Seq[Int]): Map[Int, (String, String)] = {
    import spark.implicits._

    sql(spark,
      s"""
         |SELECT a.profile_id, a.profile_name, a.os as plat
         |FROM $profileMetadata as a
      """.stripMargin)
      .map(r => (r.getAs[Int]("profile_id"), (r.getAs[String]("profile_name"), r.getAs[String]("plat"))))
      .collect().toMap
  }

  /*
   * get profile infos by profileids
   * return: profile_id, profile_name, plat
   */
  def getProfileVersionByProfileIds(spark: SparkSession, profileIds: Seq[Int]): Map[Int, Int] = {
    import spark.implicits._

    sql(spark,
      s"""
         |select a.profile_id, a.profile_version_id
         |from $profileVersion as a
         |WHERE a.profile_id IN (${profileIds.mkString(",")})
      """.stripMargin)
      .map(r => (r.getAs[Int]("profile_id"), r.getAs[Int]("profile_version_id")))
      .collect().toMap
  }

  def getCategoryId2ParentIdMap(spark: SparkSession): Map[Int, (Int, Int)] = {
    import spark.implicits._
    sql(spark,
      s"""
         |select profile_category_id, profile_category_parent_id, profile_category_level
         |from $profileCategory
      """.stripMargin)
      .map(r => r.getInt(0) -> (r.getInt(1), r.getInt(2)))
      .collect().toMap
  }

  /** 必须是timewindow的表 */
  def getValue2IdMapping(profiles: Array[ProfileInfo]): Map[String, String] = {
    profiles.map { p =>
      val value = """feature='([^']+)'""".r.findFirstMatchIn(p.profileColumn).get.subgroups.head
      value -> p.fullVersionId
    }.toMap
  }

  def getProfiles(spark: SparkSession, profileIdFullTable: String): Array[ProfileInfo] = {
    import spark.implicits._
    sql(spark,
      s"""
         |select a.profile_id, profile_version_id, profile_database,
         |  profile_table, profile_column, has_date_partition, profile_datatype,
         |  partition_type
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
        r.getAs[String]("partition_type").toInt
      )).collect()
  }

  def getProfileId2ParentCategoryIdMap(spark: SparkSession): Map[String, String] = {
    import spark.implicits._
    sql(spark,
      s"""
         |select concat(a.profile_id, '_', a.profile_version_id) as full_id,  c.profile_category_parent_id
         |from $individualProfile as a
         |inner join $profileMetadata as b
         |on a.profile_id = b.profile_id
         |inner join $profileCategory as c
         |on b.profile_category_id = c.profile_category_id
         |where (profile_table not like '%idfa%' and profile_table not like '%ios%')
         |  and b.is_avalable = 1 and b.is_visible = 1
       """.stripMargin)
      .map(r => r.getAs[String]("full_id") -> r.getAs[Int]("profile_category_parent_id").toString)
      .collect().toMap
  }

  // 注册udf,生成一个新的map类型的列,表示将若干标签聚合到一个kv里面
  def groupTagsByParentId(spark: SparkSession, _mapping: Map[String, String], fn: String): Unit = {
    val fullId2ParentCategoryIdMapBC = spark.sparkContext.broadcast(_mapping)
    spark.udf.register(fn, (m: Map[String, String]) => {
      if (null == m || m.isEmpty) {
        null
      } else {
        val mapping = fullId2ParentCategoryIdMapBC.value  // (mapping(tuple._1), tuple)
        m.toSeq.flatMap { tuple => mapping.get(tuple._1).map(pid => (pid, tuple)) }
          .groupBy(_._1)
          .map { case (k, arr) =>
            val seq = arr.map(_._2)
            k -> s"${seq.map(_._1).mkString(kvSep)}$pairSep${seq.map(_._2).mkString(kvSep)}"
          }
      }
    })
  }

  def customStr2Map(mapString: String, pairSep: String, kvSep: String): Map[String, String] = {
    if (StringUtils.isBlank(mapString)) {
      null
    } else {
      val tmp = mapString.split(pairSep, -1).flatMap { e =>
        val arr = e.split(kvSep, 2)
        if (arr.length == 2 && StringUtils.isNotBlank(arr(1))) {
          Some(arr(0), arr(1))
        } else {
          None
        }
      }.toMap[String, String]
      if (tmp.isEmpty) {
        null
      } else {
        tmp
      }
    }
  }

  def cleanConfidence(confMap: Map[String, String], valueMap: Map[String, String]): Map[String, String] = {
    if (null == confMap || confMap.size < 1 || null == valueMap || valueMap.size < 1) {
      null
    } else {
      // 如果valueMap中没有这个key也去掉该记录
      val tmp = confMap.filterKeys{ k =>
        valueMap.get(k) match {
          case Some(x) if StringUtils.isBlank(x) => false
          case Some("-1") => false
          case None => false
          case _ => true
        }
      }
      if (tmp.isEmpty) {
        null
      } else {
        tmp
      }
    }
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

  def removePid(m: Map[String, String]): Map[String, String] = {
    if (null == m || m.size < 1) {
      null
    } else {
      m.map{ case (k, v) => k.substring(k.indexOf(pSep) + 1) -> v}
    }
  }

  def groupByPid(hbasePidSetBC: Broadcast[Set[String]])(m: Map[String, String]): Map[String, String] = {
    if (null == m || m.size < 1) {
      null
    } else {
      val tmp = m.toSeq.flatMap { case (k, v) =>
        if (k.indexOf(pSep) > 0) {
          if (hbasePidSetBC.value.contains(k.substring(k.indexOf(pSep) + 1))) {
            Some(k.substring(0, k.indexOf(pSep)) -> (k.substring(k.indexOf(pSep) + 1), v))
          } else {
            None
          }
        } else {
          None
        }
      }

      if (tmp.isEmpty) {
        null
      } else {
        tmp.groupBy(_._1).mapValues { tuple =>
          val seq = tuple.map(_._2)
          s"${seq.map(_._1).mkString(kvSep)}$pairSep${seq.map(_._2).mkString(kvSep)}"
        }
      }
    }
  }
}

case class TablePartitionsManager(spark: SparkSession) {
  val tablePartitions: mutable.Map[String, Seq[String]] = mutable.Map.empty[String, Seq[String]]

  def getPartitions(tableName: String): Seq[String] = {
    if (tablePartitions.contains(tableName)) {
      tablePartitions(tableName)
    } else {
      import spark.implicits._
      val parts = TagsGeneratorHelper.sql(spark, s"show partitions $tableName").map(_.getString(0)).collect()
      tablePartitions.put(tableName, parts)
      parts
    }
  }
}
