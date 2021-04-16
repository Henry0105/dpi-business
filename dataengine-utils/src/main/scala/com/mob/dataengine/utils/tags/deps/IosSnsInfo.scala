package com.mob.dataengine.utils.tags.deps
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.DateUtils
import com.mob.dataengine.utils.tags.MaxAccumulator
import org.apache.spark.sql.{Column, DataFrame}

case class IosSnsInfo(cxt: Context) extends IosAbstractDataset(cxt) {
  override val datasetId: String = PropUtils.HIVE_TABLE_IOS_SNS_INFO
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
    val day = DateUtils.currentDay() // 没有时间字段

    sql(
      s"""
         |select idfa, gender, processtime
         |from ${PropUtils.HIVE_TABLE_IOS_SNS_INFO}
         |where ${sampleClause("idfa")}
       """.stripMargin)
  }
}
