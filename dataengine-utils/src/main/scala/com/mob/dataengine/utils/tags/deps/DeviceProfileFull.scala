package com.mob.dataengine.utils.tags.deps

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.tags.MaxAccumulator
import org.apache.spark.sql.{Column, DataFrame}

/**
 * from load_data_to_hbase.sh
 * processtime T-2
 *
 * @author juntao zhang
 */
case class DeviceProfileFull(cxt: Context) extends AbstractDataset(cxt) {

  override def timeCol: Column = dataset.col("processtime_all")

  val datasetId = PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL

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
    import cxt.spark.implicits._

    val pkgCateIdMap = sql(
      s"""
        |select apppkg, max(cate_l2_id) cate_l2_id
        |from ${PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR}
        |where version = '1000'
        |group by apppkg
      """.stripMargin)
      .toDF("apppkg", "cate_l2_id")
      .map(r => (r.getAs[String]("apppkg"), r.getAs[String]("cate_l2_id")))
      .collect().toMap

    val pkgCateIdBC = cxt.spark.sparkContext.broadcast(pkgCateIdMap)

    cxt.spark.udf.register("applist2cate", (applist: String) => {
      val cateIds = applist.split(",").flatMap(apppkg => pkgCateIdBC.value.get(apppkg))
      val info = cateIds.foldLeft(Map.empty[String, Int]) {
        case (acc, cateId) => acc + (cateId -> (acc.getOrElse(cateId, 0) + 1))
      }.toSeq
      if (0 == info.size) {
        null
      } else {
        s"""${info.map(id => s"${id._1}_0").mkString("\u0001")}\u0002${info.map(_._2).mkString("\u0001")}"""
      }
    })

    sql(
      s"""
         |SELECT device,
         |       segment,
         |       model_level,
         |       tot_install_apps,
         |       tag_list as tags,
         |       model,
         |       carrier,
         |       network,
         |       screensize,
         |       sysver,
         |       city_level,
         |       permanent_country,
         |       permanent_province,
         |       permanent_city,
         |       repayment,
         |       workplace,
         |       residence,
         |       country,
         |       province,
         |       city,
         |       applist2cate(applist) as applist_cate_tag,
         |       processtime_all
         |FROM ${PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL}
         |where device IS NOT NULL and device <> '-1' and ${sampleClause()}
      """.stripMargin)
  }
}
