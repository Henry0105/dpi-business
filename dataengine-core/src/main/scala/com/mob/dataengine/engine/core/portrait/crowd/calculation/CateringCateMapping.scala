package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

case class CateringCateMapping(jc: JobContext, sourceData: Option[SourceData])
  extends AbstractJob(jc, sourceData) {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  import spark._

//  override protected lazy val tableName: String =
//    PropUtils.HIVE_TABLE_CATERING_CATE_MAPPING
  override protected lazy val tableName: String = PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR

//  override protected val originalDFLoadSQL: String = s"select name, ${fields.mkString(",")} from $tableName"
  override protected val originalDFLoadSQL: String =
    s"""
       |SELECT pkg, apppkg, appname as name
       |FROM $tableName
       |WHERE cate_l2 = '外卖'
       |AND version = '1000'
     """.stripMargin

  val fieldsLabelMapping: Map[String, String] = if (needCal) {
    val map = lhtOpt.get.flatMap(_.elements.map(lhtf => lhtf.field -> lhtf.label)).toMap
    logger.info(s"CateringCateMapping|fieldsLabelMapping|$map")
    map
  } else Map.empty[String, String]

  @transient override lazy val originalDF: DataFrame = emptyDataFrame

  override def loadOriginalDF: DataFrame = {
    if (needCal) {
      logger.info(originalDFLoadSQL)
      sql(originalDFLoadSQL)
    } else emptyDataFrame
  }

  @transient override protected val joinedDF: DataFrame = emptyDataFrame
  @transient override protected val groupedDF: DataFrame = emptyDataFrame
  @transient override protected val finalDF: DataFrame = emptyDataFrame

  override protected def clear(moduleName: String): Unit = {}

  override protected lazy val _moduleName = "CateringLbsLabelWeekly"
}
