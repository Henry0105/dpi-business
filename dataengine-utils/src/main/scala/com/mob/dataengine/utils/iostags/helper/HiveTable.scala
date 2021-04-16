package com.mob.dataengine.utils.iostags.helper

import com.mob.dataengine.utils.iostags.beans.IosProfileInfo
import com.mob.dataengine.utils.iostags.enums.UpdateType

/**
 * @author xlmeng
 */
case class HiveTable(
                      database: String,
                      table: String,
                      key: String,
                      isPartitionTable: Boolean,
                      whereClause: String,
                      partitionClause: String
                    ) {

  def fullTableName: String = s"$database.$table"

  def updateTimeField(): String = {
    if (isPartitionTable) {
      val maybeMatch = """(\w+)=(\d+)""".r.findFirstMatchIn(partitionClause)
      if (maybeMatch.isDefined) maybeMatch.get.subgroups.head else "null"
    } else {
      "processtime"
    }
  }

  def updateTimeClause(profileInfo: IosProfileInfo): String = {
    if (UpdateType.isFull(profileInfo.updateType)) {
      "processtime"
    } else {
      updateTimeField()
    }
  }
}
