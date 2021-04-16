package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{Column, DataFrame}

/**
 * todo 为什么不从profile_full中获取cell_factory, imei是否可以从id_mapping中获取?
 */
case class DeviceTagCodIsFull(cxt: Context) extends AbstractDataset(cxt) {

  val datasetId: String = PropUtils.HIVE_TABLE_DEVICE_TAG_CODIS_FULL
  override def timeCol: Column = dataset.col("dt")

  def _dataset(): DataFrame = {
    val dt = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, dt)

    sql(
      s"""
         |select device,cell_factory,dt
         |from ${PropUtils.HIVE_TABLE_DEVICE_TAG_CODIS_FULL} where dt = '$dt' and ${sampleClause()}
       """.stripMargin
    )
  }

}
