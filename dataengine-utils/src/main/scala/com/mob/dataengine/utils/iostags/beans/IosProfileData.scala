package com.mob.dataengine.utils.iostags.beans

import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper
import org.apache.spark.sql.SparkSession

class IosProfileData(
                   val profileId: Int,
                   val profileVersionId: Int,
                   val profileDatabase: String,
                   val profileTable: String,
                   val profileColumn: String,
                   val hasDatePartition: Int = 1,
                   val profileDataType: String = "string"
                 ) extends Serializable {
  var key: String = "idfa"

  def hasTagListField: Boolean = profileColumn.contains(";") && !profileColumn.contains("=")

  def fullVersionId: String = s"${profileId}_$profileVersionId"

  def fullTableName: String = s"$profileDatabase.$profileTable"

  def isPartitionTable: Boolean = 0 != hasDatePartition

  def columnClause(kvSep: String): String = {
    val arr = profileColumn.split(";").map(_.trim)
    s"concat('$fullVersionId', '$kvSep', ${TagsGeneratorHelper.valueToStr(profileDataType, arr(0))})"
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
