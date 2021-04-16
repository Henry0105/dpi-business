package com.mob.dataengine.utils.tags.deps
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame

case class IosWorkingLivingPlace(cxt: Context) extends IosAbstractDataset(cxt) {
  override val datasetId: String = PropUtils.HIVE_TABLE_LOCATION_MONTHLY_IOS

  override def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day)

    sql(
      s"""
         |select idfa,
         |    lon_home,
         |    lat_home,
         |    country_home as home_country,
         |    province_home as home_province_code,
         |    city_home as home_city_code,
         |    area_home as home_area_code,
         |    lon_work,
         |    lat_work,
         |    country_work as work_country,
         |    province_work as work_province_code,
         |    city_work as work_city_code,
         |    area_work as work_area_code,
         |    cast(type as string) type,
         |    day
         |from ${PropUtils.HIVE_TABLE_LOCATION_MONTHLY_IOS}
         |where day='$day' and  ${sampleClause("idfa")}
       """.stripMargin)
  }
}
