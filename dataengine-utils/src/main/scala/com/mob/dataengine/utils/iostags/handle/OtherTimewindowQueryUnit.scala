package com.mob.dataengine.utils.iostags.handle

import com.mob.dataengine.utils.iostags.beans.{IosProfileInfo, QueryUnitContext}
import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper

/**
 * @author xlmeng
 */
case class OtherTimewindowQueryUnit(cxt: QueryUnitContext, profiles: Array[IosProfileInfo])
  extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    val fieldName = profile.profileColumn.split(";").map(_.trim).head
    val dataType = profile.profileDataType
    s"""
       |select ${hiveTable.key},
       |  concat('${profile.fullVersionId}', '$kvSep', ${TagsGeneratorHelper.valueToStr(dataType, fieldName)}) as kv,
       |  ${hiveTable.updateTimeClause(profile)} as update_time
       |from  ${hiveTable.fullTableName}
       |${hiveTable.whereClause}
     """.stripMargin
  }
}
