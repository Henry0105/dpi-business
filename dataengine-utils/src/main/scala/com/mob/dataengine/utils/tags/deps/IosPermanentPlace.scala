package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame

case class IosPermanentPlace(cxt: Context) extends IosAbstractDataset(cxt) {
  override val datasetId: String = PropUtils.HIVE_TABLE_IOS_PERMANENT_PLACE

  override def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day)

    sql(
      s"""
         |select idfa,
         |    permanent_country,
         |    permanent_province,
         |    permanent_city,
         |    permanent_country_cn,
         |    permanent_province_cn,
         |    permanent_city_cn,
         |    day
         |from ${PropUtils.HIVE_TABLE_IOS_PERMANENT_PLACE}
         |where day='$day' and ${sampleClause("idfa")}
       """.stripMargin)
  }
}
