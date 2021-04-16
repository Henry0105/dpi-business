package com.mob.dataengine.utils.iostags.handle

import com.mob.dataengine.utils.iostags.beans.{IosProfileInfo, QueryUnitContext}
import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.SparkSession

/**
 * @author xlmeng
 */
case class CommonQueryUnit(cxt: QueryUnitContext, profiles: Array[IosProfileInfo]) extends QueryUnit(cxt, profiles) {

  override def query(): String = {
    val ps1 = profiles.filter(_.hasTagListField)
    // 对taglist;7002_001先截取前面的,再分组
    val ps1Columns = ps1.groupBy(_.profileColumn.split(";")(0)).map { case (field, arr) =>
      processTaglistLikeFields(cxt.spark, arr, field)
    }.mkString(",")

    val ps2 = profiles.filter(!_.hasTagListField)
    val ps2Columns = TagsGeneratorHelper.buildMapStringFromFields(ps2, kvSep)

    val columns = if (ps1.isEmpty) {
      // 没有tagsList/cateList
      s"concat_ws('$pairSep', $ps2Columns)"
    } else if (ps2Columns.isEmpty) {
      // 只有tagsList/cateList
      s"concat_ws('$pairSep', $ps1Columns)"
    } else {
      s"concat_ws('$pairSep', $ps2Columns, $ps1Columns)"
    }

    s"""
       |select ${hiveTable.key}
       |     , $columns as kv
       |     , ${hiveTable.updateTimeClause(profile)} as update_time
       |from   ${hiveTable.fullTableName}
       |${hiveTable.whereClause}
     """.stripMargin
  }

  // 处理taglist这样字段的udf
  def processTaglistLikeFields(spark: SparkSession, arr: Array[IosProfileInfo], field: String): String = {
    val m = arr.map(p => p.profileColumn.split(";")(1) -> s"${p.fullVersionId}").toMap
    val mBC = spark.sparkContext.broadcast(m)
    val fn = s"process_${field}_${RandomStringUtils.randomAlphanumeric(5)}"

    spark.udf.register(s"$fn", (f: String) => {
      if (f == null) null else {
        val tmp = f.split("=")
        if (tmp.length <= 1) null else {
          val pairs = tmp(0).split(",").zip(tmp(1).split(",")).flatMap { case (tagId, v) =>
            mBC.value.get(tagId).map(fullId => s"$fullId$kvSep$v")
          }
          if (pairs.isEmpty) null else pairs.mkString(pairSep)
        }
      }
    })

    s"$fn($field)"
  }

}