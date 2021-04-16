package com.mob.dataengine.utils.iostags.handle

import com.mob.dataengine.utils.iostags.beans.{IosProfileInfo, QueryUnitContext}
import com.mob.dataengine.utils.iostags.enums.PartitionType

/**
 * @author xlmeng
 */
object QueryUnitFactory {

  def createQueryUnit(cxt: QueryUnitContext, profiles: Array[IosProfileInfo], isConfidence: Boolean = false):
  Seq[QueryUnit] = {
    if (isConfidence) {
      createConfidenceQueryUnit(cxt, profiles)
    } else {
      createTagsQueryUnit(cxt, profiles)
    }

  }

  private def createConfidenceQueryUnit(cxt: QueryUnitContext, profiles: Array[IosProfileInfo]): Seq[QueryUnit] = {
    profiles.groupBy(p => (p.fullTableName, p.period)).map {
      case (_, ps) =>
        ConfidenceQueryUnit(cxt, ps)
    }.toSeq
  }

  private def createTagsQueryUnit(cxt: QueryUnitContext, profiles: Array[IosProfileInfo]): Seq[QueryUnit] = {
    profiles.groupBy(p => (p.fullTableName, p.period)).values.zipWithIndex.flatMap { case (ps, i) =>
      // 拼接成查询标签的sql语句
      if (ps.head.isTimewindowTable) {
        ps.groupBy(_.flagTimewindow).values.zipWithIndex.map { case (ps2, j) =>
          PartitionType(ps2.head.partitionType) match {
            case PartitionType.feature => OnlyFeatureQueryUnit(cxt, ps2)
            case PartitionType.timewindowFlagFeature => TimewindowFlagFeatureQueryUnit(cxt, ps2, s"${i}_$j")
            case _ => OtherTimewindowQueryUnit(cxt, ps2)
          }
        }
      } else {
        Seq(CommonQueryUnit(cxt, ps))
      }
    }.toSeq
  }


}
