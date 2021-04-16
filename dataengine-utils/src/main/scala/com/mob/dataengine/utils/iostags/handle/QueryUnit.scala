package com.mob.dataengine.utils.iostags.handle

import com.mob.dataengine.utils.iostags.beans.{IosProfileInfo, QueryUnitContext}
import com.mob.dataengine.utils.iostags.enums.UpdateType
import com.mob.dataengine.utils.iostags.helper.{HiveTable, TagsDateProcess, TagsGeneratorHelper}
import org.apache.commons.lang3.StringUtils

/**
 * @author xlmeng
 */
abstract class QueryUnit(cxt: QueryUnitContext, profiles: Array[IosProfileInfo]) extends Serializable {

  protected val kvSep: String = TagsGeneratorHelper.kvSep
  protected val pSep: String = TagsGeneratorHelper.pSep
  protected val pairSep: String = TagsGeneratorHelper.pairSep
  val profile: IosProfileInfo = profiles.head
  protected lazy val hiveTable: HiveTable = buildHiveTableFromProfiles()
  lazy val msg = s"${profile.period}_${profile.periodDay}_${profile.profileColumn}"

  def query(): String

  def check(): (String, Boolean, String) = {
    if (hiveTable.isPartitionTable) {
      val part = getAssignPartition()
      (hiveTable.fullTableName, StringUtils.isNoneBlank(part), msg)
    } else {
      (hiveTable.fullTableName, true, msg)
    }
  }

  protected def buildHiveTableFromProfiles(): HiveTable = {
    val key = profile.key
    val partitionClause = if (profile.isPartitionTable) {
      val part = getAssignPartition()
      if (StringUtils.isNotBlank(part)) part else "false"
    } else {
      "true"
    }
    lazy val fullPartitionClause = if (profile.isPartitionTable) s"day <= ${cxt.day}" else "true"

    val updateTimeClause = getUpdateTime()

    val whereClause =
      s"""
         |where (${profiles.map(p => s"(${p.whereClause()})").toSet.mkString(" or ")})
         |  and ${if (cxt.full && UpdateType.isIncr(profile.updateType)) fullPartitionClause else partitionClause}
         |  and ${if (cxt.full) "true" else updateTimeClause}
         |  and ${sampleClause(cxt.sample)}
         """.stripMargin

    HiveTable(profile.profileDatabase, profile.profileTable, key,
      profile.isPartitionTable, whereClause, partitionClause)
  }

  /**
   * 分区目前一共4类:
   * 1: day=20200623/timewindow=14/flag=2, day=20200623/timewindow=14/flag=3, day=20200623/timewindow=7/flag=2
   * 单独处理,[[getDayByDetailPartition]]
   * 2: day=20200627/feature=5698_1000, day=20200627/feature=5699_1000, day=20200627/feature=5701_1000
   * 3: day=20200623/timewindow=14, day=20200623/timewindow=30, day=20200623/timewindow=7
   * 2个分区字段，通过非day字段来取出day字段 [[getDayByDetailPartition]]
   * 4: day=20200601
   * 直接取
   */
  protected def getAssignPartition(): String = {
    val parts = cxt.tbManager.getPartitions(profile.fullTableName)
    val dataDate = TagsDateProcess.getDataDate(cxt.day, profile.period, profile.periodDay)
    val assignParts = parts.foldLeft(List.empty[String])((xs, x) =>
      if ("""\d{8}""".r.findFirstMatchIn(x).get.matched == dataDate) x :: xs else xs
    )

    val detailPartition = profile.getDetailPartition
    if (assignParts.nonEmpty) {
      if (detailPartition.nonEmpty) {
        getDayByDetailPartition(assignParts, profile, detailPartition)
      } else {
        assignParts.head
      }
    } else ""
  }

  private def getDayByDetailPartition(parts: Seq[String], profileInfo: IosProfileInfo,
                                      detailPartition: Seq[String]): String = {
    println(s"filter ${profileInfo.fullTableName} with ${detailPartition.mkString(" and ")}")
    val fdt = parts.filter { p =>
      val arr = p.split("/")
      detailPartition.forall(arr.contains(_))
    }
    if (fdt.nonEmpty) {
      /** 对于timewindow 需要只取出day字段 */
      """day=(\d+)""".r.findFirstMatchIn(fdt.last).get.matched
    } else {
      ""
    }
  }

  // 2种情况,全量表需要取update_time(目前都是日更新),增量表直接走分区筛选
  private def getUpdateTime(): String = {
    if (UpdateType.isFull(profile.updateType)) {
      profile.profileTable match {
        case "ios_device_info_sec_df" => s" update_date = '${cxt.day}' "
        case _ => s" processtime = '${cxt.day}' "
      }
    } else {
      "true"
    }

  }

  // sample=true 是测试使用
  protected def sampleClause(sample: Boolean, key: String = "device"): String = {
    val randomType = (Math.random() * 100000).toInt
    if (sample) {
      s"hash($key)%1000000=$randomType"
    } else {
      "true"
    }
  }

}