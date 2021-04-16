package com.mob.dataengine.utils.tags.deps
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.tags.MaxAccumulator
import org.apache.spark.sql.{Column, DataFrame}

case class IdfaDeviceInfoFull(cxt: Context) extends IosAbstractDataset(cxt) {
  override val datasetId: String = PropUtils.HIVE_TABLE_IDFA_DEVICE_INFO_FULL
  override def timeCol: Column = dataset.col("processtime")

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
         |select idfa, carrier, model, screensize, sysver, price, breaked, public_date, factory, processtime
         |from ${PropUtils.HIVE_TABLE_IDFA_DEVICE_INFO_FULL}
         |where processtime is not null and ${sampleClause("idfa")}
       """.stripMargin)
  }
}
