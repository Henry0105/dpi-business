package com.mob.dataengine.utils.tags.fulltags.profile

import org.apache.spark.sql.SparkSession
import com.mob.dataengine.utils.tags.fulltags.profile.FullTagsGeneratorHelper.pSep

class ProfileData(
  val profileId: Int,
  val profileVersionId: Int,
  val profileDatabase: String,
  val profileTable: String,
  val profileColumn: String,
  val hasDatePartition: Int = 0,
  val profileDataType: String = "string"
) extends Serializable {
  def hasTagListField: Boolean = profileColumn.contains(";") && !profileColumn.contains("=")

  def fullVersionId: String = s"${profileId}_$profileVersionId"

  def parentId(fullId2PidMap: Map[String, String]): String = fullId2PidMap(fullVersionId)

  def fullTableName: String = s"$profileDatabase.$profileTable"

  def key(spark: SparkSession): String = {
    val fields = spark.table(fullTableName).schema.fields.map(_.name)
    if (fields.contains("device")) {
      "device"
    } else if (fields.contains("device_id")) {
      "device_id"
    } else if (fields.contains("deviceid")) {
      "deviceid"
    } else {
      "id"
    }
  }

  def isPartitionTable: Boolean = 0 != hasDatePartition

  def columnClause(kvSep: String): String = {
    val arr = profileColumn.split(";").map(_.trim)
    s"concat('$fullVersionId', '$kvSep', ${FullTagsGeneratorHelper.valueToStr(profileDataType, arr(0))})"
  }

  // 将父级id拼入版本号
  def pColumnClause(kvSep: String, fullId2PidMap: Map[String, String]): String = {
    val arr = profileColumn.split(";").map(_.trim)
    s"concat('${parentId(fullId2PidMap)}$pSep$fullVersionId', '$kvSep', cast(${arr(0)} as string))"
  }

  // 如果是线上/线下标签,拿到对应的 flag=7 and timewindow='40'
  // v3表为: cnt;feature='5444_1000'
  def flagTimewindow: String = {
    if (profileColumn.contains("flag=")) {
      val flag = """flag=(\d+)""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      val timeWindow = """timewindow='(\d+)'""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      s"flag=$flag and timewindow='$timeWindow'"
    } else if (profileColumn.contains("feature=")) {
      val feature = """feature='(.+)'""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      s"feature='$feature'"
    } else {
      throw new Exception("标签配置有问题: " + profileColumn)
    }
  }
}

case class ProfileConfidence (
  override val profileId: Int,
  override val profileVersionId: Int,
  override val profileDatabase: String,
  override val profileTable: String,
  override val profileColumn: String
) extends ProfileData (profileId, profileVersionId,
  profileDatabase, profileTable, profileColumn
)

case class MidengineStatus(profileId: Int, profileVersionId: Int, hivePartition: String)
