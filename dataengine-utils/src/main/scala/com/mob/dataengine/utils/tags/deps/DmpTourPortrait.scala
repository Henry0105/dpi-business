package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame

case class DmpTourPortrait(cxt: Context) extends AbstractDataset(cxt) {
  val datasetId: String = s"${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}_9_30"

  def _dataset(): DataFrame = {
    // 只有每周三跑一次
    val day_9_30 = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day_9_30)

    // todo 需要 dm_sdk_master.travel_daily maybe duplicated
    sql(
      s"""
         |SELECT b.device AS device,
         |    CONCAT(
         |       'LA000|||LB000|||LC000|||LD000|||LG000|||LH000|||LI000|||LJ000=',
         |       c.travel_area,-----大区
         |       '|||', c.country,-----国家
         |       '|||',
         |       CASE
         |          WHEN (business_flag=1 OR busi_app_act =1)
         |             THEN 'business'
         |          WHEN (poi_flag=1 AND travel_app_act=1)
         |            OR car=1 THEN 'tourself'
         |          WHEN poi_flag =1 AND (
         |                 business_flag=0
         |                 AND busi_app_act =0
         |                 AND poi_flag=0
         |                 AND travel_app_act=0
         |                 AND car=0) THEN 'tourteam'
         |          ELSE 'unknown'
         |       END, -----出行类型
         |       '|||', c.vaca_flag,----出行时段
         |       '|||', c.province_flag,------大陆地区偏好
         |       '|||', concat_ws('', b.9_province_30),-----省份
         |       '|||',
         |       CASE
         |         WHEN concat_ws('', b.9_city_30) LIKE '%中国%' THEN concat_ws('', b.9_province_30)
         |         ELSE concat_ws('', b.9_city_30)
         |       END ,------城市
         |       '|||', concat_ws('', b.9_cate1_30) -----景区
         |    ) AS portrait_tour_tag,
         |    b.day
         |FROM
         |  (SELECT device,
         |          collect_list(a.group_map['9_province_30']) AS 9_province_30,
         |          collect_list(a.group_map['9_city_30']) AS 9_city_30,
         |          collect_list(a.group_map['9_cate1_30']) AS 9_cate1_30,
         |          max(day) as day
         |   FROM
         |     (SELECT device,
         |             map(feature, cnt) AS group_map,
         |             day
         |      FROM ${PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2}
         |      WHERE flag=9
         |        AND day = '$day_9_30'
         |        AND timewindow=30 and ${sampleClause()}) a
         |   GROUP BY a.device) b
         |inner JOIN (
         |  select device,travel_area, country, business_flag, busi_app_act, poi_flag,
         |    travel_app_act, vaca_flag, province_flag, car from (
         |    select device,travel_area, country, business_flag, busi_app_act, poi_flag,
         |      travel_app_act, vaca_flag, province_flag, car,
         |      row_number() over (partition by device order by day desc) as rank
         |    from ${PropUtils.HIVE_TABLE_TRAVEL_DAILY}
         |    where ${sampleClause()}
         |  ) as tmp
         |  where rank = 1
         |) c ON b.device = c.device
       """.stripMargin)
  }

}
