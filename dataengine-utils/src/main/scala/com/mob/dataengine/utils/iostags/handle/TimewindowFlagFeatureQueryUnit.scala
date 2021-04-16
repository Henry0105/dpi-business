package com.mob.dataengine.utils.iostags.handle

import com.mob.dataengine.utils.iostags.beans.{IosProfileInfo, QueryUnitContext}
import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper

/**
 * @author xlmeng
 */
case class TimewindowFlagFeatureQueryUnit(cxt: QueryUnitContext, profiles: Array[IosProfileInfo], mock: String)
  extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    import cxt.spark.implicits._
    // e.g. "fin04_7_40" -> 1_1000
    val mWithPid = TagsGeneratorHelper.getValue2IdMapping(profiles).toSeq
    val fieldName = profile.profileColumn.split(";").map(_.trim).head
    val dataType = profile.profileDataType

    val featureMapping = s"feature_mapping_$mock"
    cxt.spark.createDataset(mWithPid)
      .toDF("feature_value", "feature_id")
      .createOrReplaceTempView(featureMapping)

    // 使用inner join来获取对应的id
    s"""
       |select /*+ BROADCASTJOIN(b) */
       |       ${hiveTable.key}
       |     , concat(b.feature_id, '$kvSep', ${TagsGeneratorHelper.valueToStr(dataType, fieldName)}) as kv
       |     , ${hiveTable.updateTimeClause(profile)} as update_time
       |from  ${hiveTable.fullTableName} as a
       |inner join $featureMapping as b
       |on a.feature = b.feature_value
       |${hiveTable.whereClause}
     """.stripMargin
  }

}