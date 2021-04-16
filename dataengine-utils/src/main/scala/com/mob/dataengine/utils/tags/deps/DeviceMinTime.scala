package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.tags.MaxAccumulator
import org.apache.spark.sql.DataFrame

/**
 * 在装
 * todo plat=1 ?
 */
case class DeviceMinTime(cxt: Context) extends AbstractDataset(cxt) {
  val datasetId: String = PropUtils.HIVE_TABLE_DEVICE_MINTIME_MAPPING

  val accumulator: MaxAccumulator = {
    val t = maxAccumulator(datasetId)
    cxt.tableStateManager.accumulators += t
    t
  }

  override def processTs(currentTsOpt: Option[String]): Option[String] = {
    cxt.timestampHandler.processTsWithAcc(
      currentTsOpt,
      lastTsOpt,
      Some(accumulator)
    )
  }


  override def _dataset(): DataFrame = {
    sql(
      s"""
         |select device, day as inital_ts, day
         |from ${PropUtils.HIVE_TABLE_DEVICE_MINTIME_MAPPING}
         |where plat=1 and day is not null and ${sampleClause()}
       """.stripMargin)
  }

}
