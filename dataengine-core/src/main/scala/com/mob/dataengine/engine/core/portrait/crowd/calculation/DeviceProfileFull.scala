package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.{Map, mutable}

/**
 * 基础标签计算逻辑, 含标签:
 * D001-D029, D034-D037
 * rp_sdk_dmp.rp_device_profile_full|无分区19.1亿|612.8G
 * dm_sdk_mapping.tag_cat_mapping_dmp_par|分区780|9.3K
 * dm_sdk_mapping.app_category_mapping_par|分区33116|3.1M
 *
 * Sum计算逻辑: 非去重设备计数
 * Cnt计算逻辑: 去重设备计数
 *
 * @see AbstractJob
 */
@author("juntao zhang")
@createTime("2018-07-04")
@sourceTable("rp_sdk_dmp.rp_device_profile_full,dm_sdk_mapping.tag_cat_mapping_dmp_par," +
  "dm_sdk_mapping.app_category_mapping_par")
case class DeviceProfileFull(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractJob(jobContext, sourceData) {
  @transient private[this] val logger = Logger.getLogger(this.getClass)
  override lazy val originalDFLoadSQL = s"select device,${fields.mkString(",")} from $tableName"
  /* 除定制化计算的其它标签 */
  private lazy val remainedFields: Set[String] = fields.filterNot("tag_list,applist,workplace,residence,special_time"
    .contains(_))
  /* 字段名称和标签名称映射关系 */
  private lazy val fieldsLabelMapping: Map[String, String] = lhtOpt.get.flatMap(
    _.elements.map(lhtf => lhtf.field -> lhtf.label)).toMap

  import spark._
  import spark.implicits._
  import spark.sparkContext.broadcast
  override protected lazy val _moduleName = "DeviceProfileFull"
  // todo MDML-25-RESOLVED 这种注释去掉
  override protected lazy val tableName: String =
    PropUtils.HIVE_TABLE_RP_DEVICE_PROFILE_FULL
  @transient
  override protected lazy val originalDF: DataFrame = loadOriginalDF
  @transient
  override protected lazy val joinedDF: DataFrame = {
    val joinedDF = loadJoinedDF.cache()
    // joinedDF.show()
    joinedDF
  }
  @transient
  override protected lazy val groupedDF: DataFrame = {
    if (remainedFields.nonEmpty) {
      logger.info(s"cal DeviceProfileFull.groupedDF with ${remainedFields.mkString("|")}")
      joinedDF.groupBy("uuid", remainedFields.toSeq: _*)
        .agg(countDistinct("device").as("cnt"), count("device").as("sum")).cache()
    }
    else emptyDataFrame
  }

  @transient
  override protected val finalDF: DataFrame = {
    val arr = mutable.ArrayBuffer[DataFrame]()
    calOthers().map(arr += _)
    calAppList().map(arr += _)
    calTags().map(arr += _)
    calWorkplaceAndResidence().map(arr += _)
    calSpecialTime().map(arr += _)
    if (arr.nonEmpty) arr.reduce(_ union _) else emptyDataFrame
  }

  /* 对D013标签和D034标签定制化计算所需数据 */
  private val broadcastMap: Broadcast[mutable.HashMap[String, Map[String, String]]] =
    if (fields.contains("applist") || fields.contains("tag_list")) {
      val m = mutable.HashMap[String, Map[String, String]]()
      appCategoryMapping(m)
      tagCatMappingDmp(m)
      broadcast(m)
    } else broadcast(mutable.HashMap.empty[String, Map[String, String]])

  // =========== 前期准备数据 ===========
  /* 标签D034前置数据准备 */
  def appCategoryMapping(m: mutable.HashMap[String, Map[String, String]]): Unit = {
    val tableName = PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR
    val field = "applist"
    if (fields.nonEmpty && fields.contains(field)) {
      // 默认使用最新版本的映射表
      val lastPar = sql(s"show partitions $tableName").collect()
        .map(_.getAs[String](0))
        .last.split("=")(1).split("\\.")(0)
      val appCategoryMappingSQL = s"select cate_l2_id,apppkg from $tableName where version='$lastPar'"
      // todo MDML-25-RESOLVED version不要硬编码
      logger.info(appCategoryMappingSQL)
      val app2Category = sql(appCategoryMappingSQL)
        .map { case Row(cat2Id: String, appPkg: String) => appPkg -> cat2Id }.collect().toMap
      logger.info(s"app2Category|partition:version=$lastPar|${app2Category.take(20)}")
      m += (field -> app2Category)
    }
  }

  /* 标签D013前置数据准备 */
  def tagCatMappingDmp(m: mutable.HashMap[String, Map[String, String]]): Unit = {
    val tableName = PropUtils.HIVE_TABLE_TAG_CAT_MAPPING_DMP_PAR
    val field = "tag_list"
    if (fields.nonEmpty && fields.contains(field)) {
      val lastPar = sql(s"show partitions $tableName").collect()
        .map(_.getAs[String](0))
        .last.split("=")(1).split("\\.")(0)
      val tagCatMappingDmpSQL = s"select cat2_id,tag_id from $tableName where version='$lastPar'"
      // todo MDML-25-RESOLVED version不要硬编码
      logger.info(tagCatMappingDmpSQL)
      val tag2Cat = sql(tagCatMappingDmpSQL).map {
        case Row(cat2Id: String, tagId: String) => tagId -> cat2Id
      }.collect().toMap
      logger.info(s"tag2Cat|partition:version=$lastPar|${tag2Cat.take(20)}")
      m += (field -> tag2Cat)
    }
  }

  /**
   * 计算标签D034
   */
  def calAppList(): Option[DataFrame] = {
    import spark.implicits._

    val f = "applist"
    if (fields.contains(f)) {
      Some(
        joinedDF.filter(col(f).isNotNull && trim(col(f)).notEqual(""))
          .select($"uuid", col(f), $"device")
          .flatMap {
            r: Row =>
              val m = broadcastMap.value(f)
              val appSplited = r.getAs[String](f).trim.split(",")
              appSplited.map(
                app => m.getOrElse(app, "")
              ).filter(_.nonEmpty).map {
                a: String => (r.getAs[String]("uuid"), r.getAs[String]("device"), a)
              }
          }.toDF("uuid", "device", "label_id")
          .filter($"label_id".isNotNull && trim($"label_id").notEqual(""))
          .groupBy("uuid", "label_id")
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("D034").as("label"), $"label_id", $"cnt", $"sum"))
    } else None
  }

  // =========== 特殊逻辑准备 ===========

  /**
   * 计算标签D013
   */
  def calTags(): Option[DataFrame] = {
    val f = "tag_list"
    if (fields.contains(f)) {
      Some(
        joinedDF.filter(null != _)
          .filter(col(f).isNotNull && trim(col(f)).notEqual(""))
          .select($"uuid", col(f), $"device")
          .flatMap {
            r =>
              val m: Map[String, String] = broadcastMap.value(f)
              val tagArr = r.getAs[String](f).trim.split(",")
              tagArr.map(
                tag => m.getOrElse(tag, "")
              ).filter(_.nonEmpty).map {
                a => (r.getAs[String]("uuid"), r.getAs[String]("device"), a)
              }
          }.toDF("uuid", "device", "label_id")
          .filter($"label_id".isNotNull && trim($"label_id").notEqual(""))
          .groupBy("uuid", "label_id")
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("D013").as("label"), $"label_id", $"cnt", $"sum"))
    } else None
  }

  /**
   * 计算标签D027, D028
   */
  def calWorkplaceAndResidence(): Option[DataFrame] = {
    val spFields = Set("workplace", "residence") intersect fields
    val fieldsLabelMapping = Map("workplace" -> "D027", "residence" -> "D028")
    if (spFields.nonEmpty) {
      Some(
        spFields.map {
          f =>
            joinedDF.select("uuid", "device", f)
              .filter(col(f).isNotNull && trim(col(f)).notEqual(""))
              /** lat:40.810078,lon:111.663174,province:cn28,city:cn28_07,area:cn28_07_04,
               * street:大南街街道,cnt:2 -> array(cn28,cn28_07,cn28_07_04) -> explode
               */
              .withColumn("label_id", explode(split(regexp_replace(col(f),
              ".*province:([\\s|\\w]*,)city:([\\s|\\w]*,)area:([\\s|\\w]*),street:.*", "$1$2$3"), ",")))
              .filter(length(trim(col("label_id"))) > 0)
              .groupBy($"uuid", $"label_id")
              .agg(count($"device").as("cnt"), countDistinct("device").as("sum"))
              .select($"uuid", lit(fieldsLabelMapping(f)).as("label"), $"label_id", $"cnt", $"sum")
        }.reduce(_ union _)
      )
    } else None
  }

  /**
   * 计算标签D036
   */
  def calSpecialTime(): Option[DataFrame] = {
    val f = "special_time"
    if (fields.contains(f)) {
      Some(
        joinedDF.filter(null != _)
          .filter(col(f).isNotNull && trim(col(f)).notEqual(""))
          .select($"uuid", col(f), $"device")
          .flatMap {
            case Row(uuid: String, tl: String, device: String) =>
              tl.split(",").map(s => (uuid, device, s)).toIterator
          }.toDF("uuid", "device", "label_id")
          .filter($"label_id".isNotNull && trim($"label_id").notEqual(""))
          .groupBy("uuid", "label_id")
          .agg(countDistinct("device").as("cnt"), count("device").as("sum"))
          .select($"uuid", lit("D036").as("label"), $"label_id", $"cnt", $"sum"))
    } else None
  }

  /**
   * 计算标签D001-D012, D014-D029, D035, D037
   */
  def calOthers(): Option[DataFrame] = {
    if (remainedFields.nonEmpty) {
      Some(
        remainedFields.map {
          f =>
            groupedDF.select("uuid", f, "cnt", "sum")
              .filter(col(f).isNotNull && trim(col(f)).notEqual(""))
              .groupBy("uuid", f)
              .agg(sum("sum").as("sum"), sum("cnt").as("cnt"))
              .select($"uuid", lit(fieldsLabelMapping(f)).as("label"), col(f).as("label_id"), $"cnt", $"sum")
        }.reduce(_ union _))
    } else None
  }

  override protected def clear(moduleName: String): Unit = {
    broadcastMap.unpersist()
    joinedDF.unpersist()
    groupedDF.unpersist()
    logger.info(s"$moduleName|clear succeeded...")
  }
}
