package com.mob.dataengine.utils.tags.deps
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.DataFrame

case class IosActiveTagList(cxt: Context) extends IosAbstractDataset(cxt) {
  override val datasetId: String = PropUtils.HIVE_TABLE_IOS_ACTIVE_TAG_LIST

  override def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day)

    sql(
      s"""
         |select idfa,
         |    tag_list,
         |    day
         |from ${PropUtils.HIVE_TABLE_IOS_ACTIVE_TAG_LIST}
         |where day='$day' and ${sampleClause("idfa")}
       """.stripMargin)
  }
}
