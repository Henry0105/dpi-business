package com.mob.dataengine.engine.core.portrait.crowd.calculation

import java.util.Calendar

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import org.apache.commons.lang.time.FastDateFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable


/**
 * 餐饮标签计算逻辑, 含标签:
 * CA000-CI000
 * dm_sdk_master.catering_lbs_label_weekly|ORC|分区2亿|12G
 * dm_sdk_mapping.catering_cate_mapping|Text|550万|252.9M
 *
 * Sum计算逻辑:
 * Cnt计算逻辑:
 *
 * @see AbstractJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
@sourceTable("dm_sdk_master.catering_lbs_label_weekly,dm_sdk_mapping.catering_cate_mapping")
case class CateringLbsLabelWeekly(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractTimewindowProfileJob(jobContext, sourceData) {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  import spark._
  import spark.implicits._

  override lazy val flag2Day: Map[String, String] = flag2DayFunc( Map(1 -> 7) )
  override lazy val flagTimeWindowMap: Map[Int, Int] = Map(1 -> 7)

  override protected lazy val tableName: String = PropUtils.HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2
  /* 映射表名称 */
  private lazy val mappingTableName: String = PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR
  /* 最后分区 */
  private lazy val lastPar: String = {
    val lastPars = sql(s"show partitions $tableName").collect().map(_.getAs[String](0))
    val flag = flagTimeWindowMap.keys.head
    val timewindow = flagTimeWindowMap.values.head
    val filterPars = lastPars.filter(p => p.contains(s"flag=$flag/") &&
      p.contains(s"/timewindow=$timewindow"))
    val days = filterPars.map(par => par.split("\\/")(1).split("=")(1) )
    days.max
  }


//  override protected lazy val originalDFLoadSQL: String =
//    s"select device, ${fields.mkString(",")} from $tableName where $lastPar"

//  private lazy val fieldsLabelMapping: Map[String, String] = {
//    val tmpMap = lhtOpt.get.flatMap(_.elements.map(lhtf => lhtf.field -> lhtf.label)).toMap
//    val res = if (labels.contains("CD000")) tmpMap + ("catering_dinein_brand_detail" -> "CD000")
//    else tmpMap.filterKeys(!_.equals("catering_dinein_brand_detail"))
//    logger.info(s"CateringLbsLabelWeekly|fieldsLabelMapping|$res")
//    res
//  }

  private val fieldsLabelMapping: Map[String, String] = {
    if (needCal) {
      logger.info(s"fieldsLabelMapping  flagTimeWindowMap=>[$flagTimeWindowMap], labels => [$labels]")
      var tmpMap = lhtOpt.get.flatMap(_.elements.filter(_.filter.isDefined)
        .map(lhtf => featureOf(lhtf.filter.get) -> lhtf.label)
        .filter(t => {
          if (StringUtils.isNotBlank(t._1) && "([a-zA-Z0-9]+_[0-9]+_[0-9]+)".r.findFirstIn(t._1).isDefined) {
            StringUtils.isNotBlank(t._1) &&
              flagTimeWindowMap.keys.toList.contains(t._1.split("_")(1).toInt) &&
              flagTimeWindowMap.values.toList.contains(t._1.split("_")(2).toInt)
          } else {
            StringUtils.isNotBlank(t._1)
          }
        }
        )).toMap

      val cntLabels = jobContext.job.params.flatMap(p => {
        p.inputs.flatMap(_.tagList)
      })
      if (cntLabels.contains("CH000")) {
        tmpMap += ("catering_takeout_cnt" -> "CH000")
      }
      if (cntLabels.contains("CI000")) {
        tmpMap += ("catering_takeout_detail" -> "CI000")
      }
      logger.info(s"CateringLbsLabelWeekly|fieldsLabelMapping|$tmpMap")
      tmpMap
    } else Map.empty[String, String]
  }

  private val needMapping: Boolean = jobContext.labelHiveTables.get(mappingTableName).isDefined

  @transient override protected lazy val originalDF: DataFrame = loadOriginalDF
  @transient override protected lazy val joinedDF: DataFrame = loadJoinedDF.cache()
  @transient override protected lazy val groupedDF: DataFrame = emptyDataFrame
//  @transient private val (mappingDF, cateFieldsLabelMapping): (DataFrame, Map[String, String]) = {
//    if (needCal) joinedDF.createOrReplaceTempView("catering_lbs_label_weekly_tmp")
//    if (needMapping) {
//      val cateringCateMapping = CateringCateMapping(jobContext, sourceData)
//      val cateringDF = sql(
//        s"""
//           |select m.uuid, m.device, t.brand_name, t.brand_cnt as sum, 1 as cnt
//           |from (
//           |   select uuid, device, catering_dinein_brand_detail
//           |   from catering_lbs_label_weekly_tmp
//           |   where catering_dinein_brand_detail is not null
//           |   and catering_dinein_brand_detail <> ''
//           |) m
//           |lateral view explode_tags(m.catering_dinein_brand_detail) t as brand_name, brand_cnt
//           |group by m.uuid, m.device, t.brand_name, t.brand_cnt
//         """.stripMargin)
//      val cateringMappingDF = cateringCateMapping.loadOriginalDF
//
//      (cateringDF.join(cateringMappingDF,
//        cateringDF("brand_name") === cateringMappingDF("name")).drop(cateringMappingDF("name")).cache()
//        -> cateringCateMapping.fieldsLabelMapping)
//    } else (emptyDataFrame, Map.empty[String, String])
//  }

  @transient private val mappingDFCateFieldsLabelMapping: Map[DataFrame, Map[String, String]] = {
    val containOther = fieldsLabelMapping.values.toSet.intersect(Set("CH000", "CI000"))

    if (needCal && containOther.nonEmpty) {
      joinedDF.createOrReplaceTempView("catering_lbs_label_weekly_tmp")
    }

    var initialDFMap = Map(emptyDataFrame -> Map.empty[String, String])

    if (needMapping && containOther.nonEmpty) {
      // val tmpLastParDay = lastPar.split("\\/")(1).split("=")(1)
      val lastParDay = lastPar
      val fdf = FastDateFormat.getInstance("yyyyMMdd")
      val day = org.apache.commons.lang3.time.DateUtils.parseDate(lastPar, "yyyyMMdd")
      val calendar = Calendar.getInstance()
      calendar.setTime(day)
      calendar.add(Calendar.DAY_OF_MONTH, -7)
      val startParDay = fdf.format(calendar.getTime)
      val cateringCateMapping = CateringCateMapping(jobContext, sourceData)
      cateringCateMapping.loadOriginalDF.createOrReplaceTempView("tmp_map")
      var takeoutCntDF = emptyDataFrame
      if (containOther.contains("CH000")) {
        takeoutCntDF = sql(
          s"""
             |SELECT tmp_tbl.uuid
             |    ,tmp_tbl.device
             |    ,dan.pkg AS catering_takeout_cnt
             |    ,count(dan.pkg) AS sum
             |    ,1 AS cnt
             |FROM catering_lbs_label_weekly_tmp tmp_tbl
             |JOIN (
             |    SELECT device, pkg
             |    FROM ${PropUtils.HIVE_TABLE_DEVICE_APPLIST_NEW}
             |    WHERE day >= $startParDay and day <= $lastParDay
             |    ) dan
             |ON tmp_tbl.device = dan.device
             |JOIN tmp_map t_map
             |ON dan.pkg = t_map.pkg
             |GROUP BY tmp_tbl.uuid
             |    ,tmp_tbl.device
             |    ,dan.pkg
         """.stripMargin).cache()
      }

      var takeoutDetailDF = emptyDataFrame
      if (containOther.contains("CI000")) {
        takeoutDetailDF = sql(
          s"""
             |SELECT tmp_tbl.uuid
             |    ,tmp_tbl.device
             |    ,t_map.name AS catering_takeout_detail
             |    ,count(dan.pkg) AS sum
             |    ,1 AS cnt
             |FROM catering_lbs_label_weekly_tmp tmp_tbl
             |JOIN (
             |    SELECT device, pkg
             |    FROM ${PropUtils.HIVE_TABLE_DEVICE_APPLIST_NEW}
             |    WHERE day >= $startParDay and day <= $lastParDay
             |    ) dan
             |ON tmp_tbl.device = dan.device
             |JOIN tmp_map t_map
             |ON dan.pkg = t_map.pkg
             |GROUP BY tmp_tbl.uuid
             |    ,tmp_tbl.device
             |    ,t_map.name
         """.stripMargin).cache()
      }

      if ( takeoutCntDF.count() > 0 ) {
        initialDFMap += (takeoutCntDF -> fieldsLabelMapping.filter(kv => kv._2 == "CH000" ))
      }

      if (takeoutDetailDF.count() > 0 ) {
        initialDFMap += (takeoutDetailDF -> fieldsLabelMapping.filter(kv => kv._2 == "CI000" ))
      }

      initialDFMap
    } else initialDFMap
  }

  @transient override protected lazy val finalDF: DataFrame = {
    val arr = mutable.ArrayBuffer[DataFrame]()
    arr += calOthersDataFrame
//    calOthers().map(arr += _)
    calMapping().map(arr += _)
    calCnt().map(df => arr += df)
    if (arr.nonEmpty) arr.reduce(_ union _) else emptyDataFrame
  }

  override protected def clear(moduleName: String): Unit = {
    joinedDF.unpersist()
    if (needMapping) mappingDFCateFieldsLabelMapping.keys.foreach(_.unpersist())
    logger.info(s"$moduleName|clear succeeded...")
  }

  /**
   * 计算标签CA000/CB000/CC000/CE000
   * (join表dm_sdk_mapping.catering_cate_mapping后计算)
   */
  private def calMapping(): Option[DataFrame] = {
    val mapValues = mappingDFCateFieldsLabelMapping.values.flatMap(_.values).mkString(",")
    if (mapValues.length > 0) {
      Some(
        mappingDFCateFieldsLabelMapping.filter(_._1.count() > 0).map {
          case (mappingDF, cateFieldLabelMap) =>
            val cateField = cateFieldLabelMap.keys.head
            val label = cateFieldLabelMap.values.head
            mappingDF.select($"uuid", $"device", col(cateField), $"cnt")
              .filter(col(cateField).isNotNull
                && trim(col(cateField)).notEqual(""))
              .groupBy("uuid", cateField)
              .agg(
                sum("cnt").as("cnt"),
                sum(cateField).as("sum") )
              .select($"uuid",
                lit(label).as("label"),
                col(cateField).as("label_id"),
                $"cnt", $"sum")
        }.reduce(_ union _)
      )
    } else None
  }

  /**
   * 计算标签CF000/CH000 餐饮标签-堂食频次/外卖频次
   */
//  private def calCnt(): Option[DataFrame] = {
//    val cntFields: Set[String] = fields.intersect(Set("catering_dinein_cnt", "catering_takeout_cnt"))
//    if (cntFields.nonEmpty) {
//      val res = Some(
//        cntFields.map {
//          field => {
//            joinedDF.select($"uuid", $"device", col(field))
//              .filter(col(field).isNotNull)
//              /* 外卖/堂食频次编码: 1（几乎没有）、2（每周2次）、3（每周3次）、4（每周4次）、5（几乎每天） */
//              .select($"uuid", $"device", when(col(field) > 4, 5)
//              .when(col(field) < 2, 1).otherwise(col(field)).as("label_id"))
//              .groupBy($"uuid", $"label_id")
//              .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
//              .select($"uuid", lit(fieldsLabelMapping(field)).as("label"), $"label_id", $"cnt", $"sum")
//          }
//        }.reduce(_ union _)
//      )
//      res
//    } else None
//  }

  private def calCnt(): Option[DataFrame] = {
    val cntField = "catering_takeout_cnt"
    if (fieldsLabelMapping.contains(cntField)) {
      val mappingDF = mappingDFCateFieldsLabelMapping
        .filter(_._2.contains("catering_takeout_cnt"))
        .keys.head
      var res = emptyDataFrame
      if (mappingDF.count() > 0) {
        res = mappingDF.select($"uuid", $"device", col(cntField))
            .filter(col(cntField).isNotNull)
            /* 外卖/堂食频次编码: 1（几乎没有）、2（每周2次）、3（每周3次）、4（每周4次）、5（几乎每天） */
            .select($"uuid", $"device", when(col(cntField) > 4, 5)
            .when(col(cntField) < 2, 1).otherwise(col(cntField)).as("label_id"))
            .groupBy($"uuid", $"label_id")
            .agg(
              countDistinct("device").as("cnt"),
              count("device").as("sum") )
            .select($"uuid",
              lit(fieldsLabelMapping(cntField)).as("label"),
              $"label_id", $"cnt", $"sum")
      }
      Some(res)
    } else None
  }

  // 计算所有含有filter的field
  @transient protected lazy val calOthersDataFrame: DataFrame = {
    val cntFields: Set[String] = fieldsLabelMapping.values
      .filter(value => value != "CH000" && value != "CI000" ).toSet
    if (cntFields.nonEmpty) {
      joinedDF.filter($"cnt".isNotNull && trim($"cnt").notEqual(""))
        .createOrReplaceTempView("timewindow_offline_profile_explode_tmp")

      val fieldsMappingBC = spark.sparkContext.broadcast(fieldsLabelMapping)
      spark.udf.register("feature2Label", (feature: String) => {
        fieldsMappingBC.value(feature)
      })
      sql(
        s"""
           |SELECT uuid, feature, t.label_id, t.label_cnt AS sum, 1 AS cnt
           |FROM timewindow_offline_profile_explode_tmp
           |LATERAL VIEW explode_tags(cnt) t AS label_id, label_cnt
           |WHERE t.label_id IS NOT NULL AND t.label_id <> ''
           |GROUP BY uuid, feature, t.label_id, t.label_cnt
       """.stripMargin
        )
        .groupBy("uuid", "feature", "label_id")
        .agg(
          sum("cnt").as("cnt"),
          sum("sum").as("sum") )
        .filter(
          $"feature".isNotNull &&
            trim($"feature").notEqual("") ).createOrReplaceTempView("tmp_df")
      val tmpDF = sql(
        s"""
           |SELECT uuid, feature2Label(feature) AS label, label_id, cnt, sum
           |FROM tmp_df
         """.stripMargin)
      tmpDF
    } else emptyDataFrame
  }

  /**
   * 计算标签CD000/CG000/CI000
   */
//  private def calOthers(): Option[DataFrame] = {
//    val cntFields: Set[String] =
//      if (labels.contains("CD000")) {
//        fields.intersect(Set("catering_dinein_brand_detail",
//          "catering_dinein_time_detail",
//          "catering_takeout_detail"))
//      } else fields.intersect(Set("catering_dinein_time_detail", "catering_takeout_detail"))
//
//    if (cntFields.nonEmpty) {
//      Some(
//        cntFields.map {
//          field =>
//            sql(
//              s"""
//                 |select m.uuid, m.device, t.label_id, t.label_cnt as sum
//                 |from (
//                 |   select uuid, device, $field
//                 |   from catering_lbs_label_weekly_tmp
//                 |   where $field is not null and $field <> ''
//                 |) m
//                 |lateral view explode_tags(m.$field) t as label_id, label_cnt
//                 |group by m.uuid, m.device, t.label_id, t.label_cnt
//             """.stripMargin)
//              .filter($"label_id".isNotNull && trim($"label_id").notEqual(""))
//              .groupBy($"uuid", $"label_id")
//              .agg(countDistinct($"device").as("cnt"), sum("sum").as("sum"))
//              .select($"uuid", lit(fieldsLabelMapping(field)).as("label"), $"label_id", $"cnt", $"sum")
//        }.reduce(_ union _)
//      )
//    } else None
//  }


  override protected  lazy val _moduleName = "CateringLbsLabelWeekly"
}
