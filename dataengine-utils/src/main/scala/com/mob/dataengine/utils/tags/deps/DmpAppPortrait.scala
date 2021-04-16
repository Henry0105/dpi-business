package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame

/**
 * dmp_app_portrait_hbase.sh
 */
case class DmpAppPortrait(cxt: Context) extends AbstractDataset(cxt) {
  val datasetId: String = s"${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_4_7"

  def _dataset(): DataFrame = {
    val installedAppTagPar = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, installedAppTagPar)
    sql(
      s"""
         |select device,concat(
         |    concat_ws('|||', collect_list(concat(substring(feature,1,4),'_',substring(feature,5,3),'_0'))),
         |    '=',
         |    concat_ws('|||', collect_list(cnt))
         |    )  as installed_app_tag,
         |    max(day) as day
         |from ${PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}
         |where flag=4 ---在装
         |    and timewindow=7 and length(trim(feature))=11 and day = '$installedAppTagPar' and ${sampleClause()}
         |group  by device
      """.stripMargin)
  }

}
