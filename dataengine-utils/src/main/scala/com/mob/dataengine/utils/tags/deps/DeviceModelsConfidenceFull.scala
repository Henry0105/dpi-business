package com.mob.dataengine.utils.tags.deps
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.tags.MaxAccumulator
import org.apache.spark.sql.{Column, DataFrame}

case class DeviceModelsConfidenceFull (cxt: Context) extends AbstractDataset(cxt) {
  override def timeCol: Column = dataset.col("processtime")

  val datasetId = PropUtils.HIVE_TABLE_DEVICE_MODELS_CONFIDENCE_FULL_VIEW

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
         |SELECT device,
         |  gender,
         |  gender_cl,
         |  agebin,
         |  agebin_cl,
         |  edu,
         |  edu_cl,
         |  income,
         |  income_cl,
         |  kids,
         |  kids_cl,
         |  car,
         |  car_cl,
         |  house,
         |  house_cl,
         |  married,
         |  married_cl,
         |  occupation,
         |  occupation_cl,
         |  industry,
         |  industry_cl,
         |  agebin_1001,
         |  agebin_1001_cl,
         |  if(length(trim(cate_preference_list)) > 0, cate_preference_list, null) cate_preference_list,
         |  processtime
         |FROM ${PropUtils.HIVE_TABLE_DEVICE_MODELS_CONFIDENCE_FULL_VIEW}
         |where device IS NOT NULL and device <> '-1' and ${sampleClause()}
      """.stripMargin)
  }
}
