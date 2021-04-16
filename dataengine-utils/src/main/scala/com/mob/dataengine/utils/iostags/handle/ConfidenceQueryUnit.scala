package com.mob.dataengine.utils.iostags.handle

import com.mob.dataengine.utils.iostags.beans.{IosProfileInfo, QueryUnitContext}
import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper
/**
 * @author xlmeng
 */
case class ConfidenceQueryUnit(cxt: QueryUnitContext, profiles: Array[IosProfileInfo])
  extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    s"""
       |select ${hiveTable.key}
       |     , concat_ws('$pairSep', ${TagsGeneratorHelper.buildMapStringFromFields(profiles, kvSep)}) as confidence
       |     , ${hiveTable.updateTimeClause(profile)} as update_time
       |from ${hiveTable.fullTableName}
       |${hiveTable.whereClause}
        """.stripMargin
  }

}