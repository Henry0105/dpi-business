package com.mob.dataengine.utils.iostags.beans

import com.mob.dataengine.utils.iostags.enums.PartitionType

case class IosProfileInfo(
                           override val profileId: Int,
                           override val profileVersionId: Int,
                           override val profileDatabase: String,
                           override val profileTable: String,
                           override val profileColumn: String,
                           override val hasDatePartition: Int = 1,
                           override val profileDataType: String,
                           period: String,
                           periodDay: Int,
                           updateType: String,
                           partitionType: Int) extends IosProfileData(profileId, profileVersionId,
  profileDatabase, profileTable, profileColumn, hasDatePartition, profileDataType) {

  def whereClause(): String = {
    val arr = profileColumn.split(";").map(_.trim)
    if (isTimewindowTable) {
      // 如果是timewindow的表,需要去掉feature字段
      if (profileColumn.contains("flag=")) {
        val flag = """flag=(\d+)""".r.findFirstMatchIn(profileColumn) match {
          case Some(s) => s
          case None => "true"
        }

        val timewindow = """timewindow='(\d+)'""".r.findFirstMatchIn(profileColumn) match {
          case Some(s) => s
          case None => "true"
        }
        s"$flag and $timewindow"
      } else { // v3表
        profileColumn.split(";")(1)
      }
    } else if (arr.length > 1 && !hasTagListField) {
      arr(1)
    } else {
      "true"
    }
  }

  /** 从profile_column字段解析出需要的详细分区字段 */
  def getDetailPartition: Seq[String] = {
    val column = profileColumn
    lazy val segment = column.split(";")
    lazy val fields = segment(1).split("and")
    val noSegments = Array("tag_list", "catelist", "tag_tfidf")
    val noParFields = Array("plat")

    if (column.contains("flag") && column.contains("timewindow")) {
      val flag = """flag=(\d+)""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      val timeWindow = """timewindow='(\d+)'""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      Seq(s"flag=$flag", s"timewindow=$timeWindow")
    } else if (column.contains(";") && fields.length == 1 && !noSegments.contains(segment.head) &&
      !noParFields.contains(fields(0).split("=")(0).trim)) {
      val fieldKey = profileColumn.split(";")(1).split("=")(0).trim
      val fieldValue = s"""$fieldKey='(.+)'""".r.findFirstMatchIn(profileColumn).get.subgroups.head
      Seq(s"$fieldKey=$fieldValue")
    } else {
      Seq()
    }
  }

  /**
   * 1: 代表按天更新 取分区
   * 2: 应该指的是 取分区 但是分区不是按天更新的
   */
  def isTimewindowTable: Boolean = {
    PartitionType.isTimewinodw(partitionType)
  }

  def hasFeature: Boolean = {
    PartitionType.hasFeature(partitionType)
  }

  def isFeature: Boolean = {
    PartitionType.isFeature(partitionType)
  }

  def isTimewindowFlagFeature: Boolean = {
    PartitionType.isTimewindowFlagFeature(partitionType)
  }
}
