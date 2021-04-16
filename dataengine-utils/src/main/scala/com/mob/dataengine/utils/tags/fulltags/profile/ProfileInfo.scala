package com.mob.dataengine.utils.tags.fulltags.profile

import com.mob.dataengine.utils.tags.fulltags.enums.PartitionType

case class ProfileInfo(
                        override val profileId: Int,
                        override val profileVersionId: Int,
                        override val profileDatabase: String,
                        override val profileTable: String,
                        override val profileColumn: String,
                        override val hasDatePartition: Int,
                        override val profileDataType: String,
                        updateType: String,
                        partitionType: Int) extends ProfileData(profileId, profileVersionId,
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
