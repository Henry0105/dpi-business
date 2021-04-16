package com.mob.dataengine.utils.tags.deps
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.{DataFrame, Row}

case class TravelLabelMonthly(cxt: Context) extends AbstractDataset(cxt) {
  override val datasetId: String = PropUtils.HIVE_TABLE_TRAVEL_LABEL_MONTHLY

  override def _dataset(): DataFrame = {
    val day = cxt.tablePartitions(datasetId)
    cxt.update(datasetId, day)

    import cxt.spark.implicits._

    val tagIdMappingBC = cxt.spark.sparkContext.broadcast(getTagIdMapping())
    val sep = "\u0001"

    val fields = getTagIdMapping().keys.toSeq
    val tagId2ValueMap = s"map(${getTagIdMapping().map{ case (k, v) => s"'$k', '$v'"}.mkString(",")})"

    // concat_ws('\u0002', concat_ws('\u0001', map[f1], map[f2]), concat_ws('\u0001', f1, f2))
    val collapseFieldsSQL =
      s"""
         |concat_ws('\u0002',
         |  concat_ws('$sep', ${fields.map(f => s"$tagId2ValueMap['$f']").mkString(",")}),
         |  concat_ws('$sep', ${fields.mkString(",")})
         |)
       """.stripMargin

    sql(
      s"""
         |select device, compact_tags($collapseFieldsSQL), day
         |from ${PropUtils.HIVE_TABLE_TRAVEL_LABEL_MONTHLY}
         |where day='$day' and ${sampleClause()}
       """.stripMargin)
      .toDF("device", "cateid_39", "day")
      .filter("length(cateid_39)>1")
  }

  def getTagIdMapping(): Map[String, String] = {
    Map(
      "travel_area" -> "5331_1000", "country" -> "5332_1000", "travel_type" -> "5333_1000",
      "vaca_flag" -> "5334_1000", "province_flag" -> "5335_1000", "province" -> "5336_1000",
      "city" -> "5337_1000", "travel_time" -> "5338_1000", "traffic" -> "5339_1000",
      "travel_channel" -> "5340_1000"
    )
  }
}
