package com.mob.dataengine.utils.idmapping.hive

import java.util.regex.Pattern

import com.mob.dataengine.commons.utils.{FnHelper, Md5Helper, PropUtils}
import com.mob.dataengine.utils.{DateUtils, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class IdMappingExternalFull(@transient spark: SparkSession, day: String, once: Option[Boolean] = Some(false),
                            test: Boolean = false)
  extends Serializable {

  @transient private[this] val logger = Logger.getLogger(this.getClass)
  val srcTable: String = PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC
  val targetTable: String = PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL
  val targetTable2: String = PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC
  def sql(sql: String): DataFrame = {
    logger.info(">>>>>>>>>>>>>>>>>")
    logger.info(sql)
    val df = spark.sql(sql)
    logger.info("<<<<<<<<<<<<<<\n\n")
    df
  }

  def prepare(): Unit = {
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    // 对type字段进行反转,需要考虑 imei_14_phone, phone_imei_14
    spark.udf.register("reverse_type", reverseType _)
  }

  // 对类型进行反转
  def reverseType(s: String): String = {
    val arr = s.split("_")
    if (arr.length == 2) {
      s"${arr(1)}_${arr.head}"
    } else {  // 其实不会走到这一步
      if (arr(1).equals("14") || arr(1).equals("15")) {  // imei_14_phone
        s"${arr(2)}_${arr.head}_${arr(1)}"
      } else { // phone_imei_14
        s"${arr(1)}_${arr(2)}_${arr.head}"
      }
    }
  }

  def md5(s: String): String = {
    if (StringUtils.isBlank(s)) {
      null
    } else {
      Md5Helper.entryMD5_32(s.trim)
    }
  }

  /**
   * 将外部交换过来进行以下处理
   * 1. 添加密文字段,并扩充schema
   * 2. 将交换过来的数据进行反向
   * 3. 对imei/imei14/imei15进行相互补充
   */

  def padMd5Field(): DataFrame = {
    import spark.implicits._
    val dayFilter = if (once.nonEmpty && once.get) s" day <= '$day'" else s" day = '$day'"

    sql(
      s"""
         |select trim(owner_data) owner_data, trim(ext_data) ext_data, processtime, type
         |from $srcTable
         |where $dayFilter
       """.stripMargin).map{ r =>
      val ownerData = r.getAs[String]("owner_data")
      val extData = r.getAs[String]("ext_data")
      val processTime = r.getAs[String]("processtime")
      val tp = r.getAs[String]("type")

      padMd5Field(ownerData, extData, processTime, tp)

    }.toDF("owner_data", "owner_data_md5", "ext_data", "ext_data_md5", "processtime", "type")
  }

  def padMd5Field(ownerData: String, extData: String, processTime: String,
    tp: String): (String, String, String, String, String, String) = {
    val cleanTp = tp.replace("md5", "")

    val tuple = if (tp.endsWith("md5") && tp.contains("md5_")) {  // phonemd5_imeimd5
      (null, ownerData, null, extData, processTime, cleanTp)
    } else if (tp.endsWith("md5")) {  // phone_imeimd5
      (ownerData, md5(ownerData), null, extData, processTime, cleanTp)
    } else if (tp.contains("md5_")) {  // phonemd5_imei
      (null, ownerData, extData, md5(extData), processTime, cleanTp)
    } else {  // phone_imei
      (ownerData, md5(ownerData), extData, md5(extData), processTime, cleanTp)
    }

    (tuple._1, tuple._2, tuple._3, tuple._4, processTime, cleanTp)
  }

  def reverseFields(tuple: (String, String, String, String,
    String, String)): (String, String, String, String, String, String) = {
    (tuple._3, tuple._4, tuple._1, tuple._2, tuple._5, reverseType(tuple._6))
  }

  def substring(s: String, start: Int, length: Int): String = {
    if (StringUtils.isBlank(s) || start + length > s.length) {
      null
    } else {
      s.substring(start, start + length)
    }
  }

  def addMoreImeiFields(tuple: (String, String, String,
    String, String, String)): Seq[(String, String, String, String, String, String)] = {
    val (ownerData, ownerDataMd5, extData, extDataMd5, processTime, tp) = tuple

    val tmp = if (tp.contains("14_") || tp.endsWith("14")) { // imei14_phone -> imei_phone; phone_imei14 -> phone_imei
      Seq((ownerData, ownerDataMd5, extData, extDataMd5, processTime, tp.replace("14", "")))
    } else if (tp.contains("15_")) { // imei15_phone -> imei_phone(15位),imei_phone(14位), imei14_phone
      Seq(
        (substring(ownerData, 0, 14), md5(substring(ownerData, 0, 14)),
          extData, extDataMd5, processTime, tp.replace("15", "14")),
        (substring(ownerData, 0, 14), md5(substring(ownerData, 0, 14)),
          extData, extDataMd5, processTime, tp.replace("15", "")),
        (ownerData, ownerDataMd5, extData, extDataMd5, processTime, tp.replace("15", ""))
      )
    } else if (tp.contains("imei_")) { // imei_phone -> imei14_phone,imei15_phone, imei->phone
      Seq(
        (substring(ownerData, 0, 14), md5(substring(ownerData, 0, 14)),
          extData, extDataMd5, processTime, tp.replace("imei", "imei14")),
        (substring(ownerData, 0, 15), md5(substring(ownerData, 0, 15)),
          extData, extDataMd5, processTime, tp.replace("imei", "imei15")),
        (substring(ownerData, 0, 14), md5(substring(ownerData, 0, 14)),
            extData, extDataMd5, processTime, tp),
        (substring(ownerData, 0, 15), md5(substring(ownerData, 0, 15)),
          extData, extDataMd5, processTime, tp)
      )
    } else if (tp.endsWith("15")) { // phone_imei15 -> phone_imei, phone_imei14
      Seq(
        (ownerData, ownerDataMd5, substring(extData, 0, 14),
          md5(substring(extData, 0, 14)), processTime, tp.replace("15", "14")),
        (ownerData, ownerDataMd5, substring(extData, 0, 14),
          md5(substring(extData, 0, 14)), processTime, tp.replace("15", "")),
        (ownerData, ownerDataMd5, extData, extDataMd5, processTime, tp.replace("15", ""))
      )
    } else if (tp.endsWith("imei")) { // phone_imei -> phone_imei14,phone_imei15,phone->imei
      Seq(
        (ownerData, ownerDataMd5, substring(extData, 0, 14),
          md5(substring(extData, 0, 14)), processTime, tp.replace("imei", "imei14")),
        (ownerData, ownerDataMd5, substring(extData, 0, 15),
          md5(substring(extData, 0, 15)), processTime, tp.replace("imei", "imei15")),
        (ownerData, ownerDataMd5, substring(extData, 0, 14),
          md5(substring(extData, 0, 14)), processTime, tp),
        (ownerData, ownerDataMd5, substring(extData, 0, 15),
          md5(substring(extData, 0, 15)), processTime, tp)
      )
    } else {
      Seq(tuple)
    }

    tmp.filter(t => StringUtils.isNotBlank(t._2) && StringUtils.isNotBlank(t._4))
  }


  def createSourceTmpTable(): Unit = {
    import spark.implicits._
    val dayFilter = if (once.nonEmpty && once.get) s" day <= '$day'" else s" day = '$day'"

    sql(
      s"""
         |select trim(owner_data) owner_data, trim(ext_data) ext_data, processtime, type
         |from $srcTable
         |where $dayFilter
       """.stripMargin).flatMap{ r =>
      val ownerData = r.getAs[String]("owner_data")
      val extData = r.getAs[String]("ext_data")
      val processTime = r.getAs[String]("processtime")
      val tp = r.getAs[String]("type")

      val r1 = padMd5Field(ownerData, extData, processTime, tp)
      val r2 = reverseFields(r1)
      val r1s = addMoreImeiFields(r1)
      val r2s = addMoreImeiFields(r2)

      Seq(r1, r2) ++ r1s ++ r2s
    }.toDF("owner_data", "owner_data_md5", "ext_data", "ext_data_md5", "processtime", "type")
      .createOrReplaceTempView("tmp_source")

    val datasetRaw: DataFrame = sql(
      s"""
         |select owner_data, owner_data_md5,
         |  collect_list(CASE WHEN ext_data IS NULL THEN '' ELSE ext_data END) as ext_data,
         |  collect_list(ext_data_md5) as ext_data_md5,
         |  collect_list(processtime) as ext_data_tm,
         |  regexp_replace(type,'imei1','imei_1') type
         |from (
         |  select owner_data, owner_data_md5, ext_data, ext_data_md5, processtime, type,
         |    row_number() over (partition by owner_data, owner_data_md5,
         |    ext_data, ext_data_md5, type order by processtime asc) rn
         |  from tmp_source
         |) as a
         |where rn = 1
         |group by owner_data, owner_data_md5, type
       """.stripMargin)

    // 测试环境取部分数据
    val dataset: DataFrame = if (test) {
      datasetRaw.filter(r => {
        val a = r.getString(0).charAt(39)
        // sample
        a == '0' || a == '6' || a == 'b'
      })
    } else datasetRaw

    dataset.persist(StorageLevel.DISK_ONLY_2).createOrReplaceTempView("ext_incr")
  }


  def run(): Unit = {
    val typeList = getTypeList(srcTable, Some(true)).union(getTypeList(targetTable)).distinct
    println(typeList.mkString(","))
    createSourceTmpTable()
    val currentPar = if (day > lastPar) day else lastPar
    val toDay = DateUtils.format(DateUtils.getDayTimeStamp(day, 0).toLong * 1000)
    for (typeName <- typeList) {
      bakData(typeName)
    }
    createFullTbl(typeList)
    sql(transformSql).cache().createOrReplaceTempView("full_ext_merge")
    for (typeName <- typeList) {
      instertSqlData2(typeName, toDay)
    }
    SparkUtils.createView(spark, targetTable2, toDay, "day")
    insertSqlDay(typeList, currentPar)
    SparkUtils.createView(spark, targetTable, currentPar, "day")
  }

  def instertSqlData2(typeName: String, beforeDay: String): Unit = {
    sql(
      s"""
         |insert overwrite table $targetTable2
         |  partition(day, type)
         |select
         |  owner_data,
         |  owner_data_md5,
         |  ext_data,
         |  ext_data_md5,
         |  trans_date(ext_data_tm) ext_data_tm,
         |  '$beforeDay' day,
         |  type
         |from ext_incr
         |where type = '$typeName'
      """.stripMargin)
  }


  def stop(): Unit = {
    spark.stop()
  }

  val lastPar: String = if (once.nonEmpty && once.get) day else {
    val lastPars = sql(s"show partitions $targetTable").collect().map(_.getAs[String](0))
    val days = lastPars.map(par => par.split("\\/")(0).split("=")(1) ).distinct
    if (days.length == 0) day
    else days.max
  }

  def getTypeList(table: String, isSrc: Option[Boolean] = Some(false)): Array[String] = {
    val lastPars = sql(s"show partitions $table").collect().map(_.getAs[String](0))
    val types = lastPars.map(par => par.split("\\/")(1).split("=")(1)
      .replaceAll("md5", ""))
    if (types.length == 0) Array.empty[String]
    else types.flatMap(tmpType => {
      if (isSrc.nonEmpty && isSrc.get) {
        Seq(tmpType,
          Pattern.compile("^([\\w'-]+)[_]+([\\w'-]+)", Pattern.MULTILINE)
            .matcher(tmpType).replaceAll("$2_$1")  // 做个相反的出来
        )
      } else Seq(tmpType)
    }).distinct.map(_.replaceAll("imei1", "imei_1"))
      .flatMap{ p =>
        Seq(p, p.replaceAll("imei_([a-z]+)", "imei_14_$1"),
          p.replaceAll("imei_([a-z]+)", "imei_15_$1"),
          p.replaceAll("imei_14", "imei"),
          p.replaceAll("imei_15", "imei"))
      }
  }

  def createFullTbl(typeList: Seq[String]): Unit = {
    typeList.map(typeName =>
      sql(
        s"""
           |SELECT owner_data,
           |    owner_data_md5,
           |    ext_data,
           |    ext_data_md5,
           |    ext_data_tm,
           |    '$typeName' AS type
           |FROM ${typeName}_tmp_tbl
         """.stripMargin)).reduce(_.union(_)).createOrReplaceTempView("full_tbl")
  }

  val transformSql: String = {
    spark.udf.register("merge_inc2full", mergeIncr2Full _)

    s"""
       |select owner_data, owner_data_md5, zipped._1 ext_data, zipped._2 ext_data_md5,
       |  zipped._3 ext_data_tm, type
       |from (
       |  select
       |    coalesce(full_tbl.owner_data,ext_incr.owner_data) owner_data,
       |    coalesce(full_tbl.owner_data_md5,ext_incr.owner_data_md5) owner_data_md5,
       |    merge_inc2full(full_tbl.ext_data, ext_incr.ext_data,
       |      full_tbl.ext_data_md5, ext_incr.ext_data_md5,
       |      full_tbl.ext_data_tm, trans_date(ext_incr.ext_data_tm)) zipped,
       |    coalesce(full_tbl.type,ext_incr.type) type
       |  from full_tbl
       |  full join ext_incr
       |  on full_tbl.owner_data_md5 = ext_incr.owner_data_md5
       |    and coalesce(full_tbl.type,ext_incr.type) = ext_incr.type
       |) merged
      """.stripMargin
  }

  // 将增量表的数据合并入全量表, 以md5字段来zip再合并
  def mergeIncr2Full(fullExtData: Seq[String], incExtData: Seq[String],
    fullExtDataMd5: Seq[String], incExtDataMd5: Seq[String],
    fullTm: Seq[String], incTm: Seq[String]): (Seq[String], Seq[String], Seq[String]) = {
    if (fullExtDataMd5 == null || fullExtDataMd5.isEmpty) {
      (incExtData, incExtDataMd5, incTm)
    } else if (incExtDataMd5 == null || incExtDataMd5.isEmpty) {
      (fullExtData, fullExtDataMd5, fullTm)
    } else { // 合并数据
      val fullZip = fullExtDataMd5.zip(fullExtData).zip(fullTm).map{ case ((vMd5, v), tm) => (vMd5, (v, tm))}.toMap
      val incZip = incExtDataMd5.zip(incExtData).zip(incTm).map{ case ((vMd5, v), tm) => (vMd5, (v, tm))}.toMap

      val res = incZip.foldLeft(fullZip) { case (acc, inc) =>
          if (acc.contains(inc._1)) {  // 更新对应的时间戳
            if (acc(inc._1)._2 > inc._2._2) {
              acc ++ Map(inc._1 -> inc._2)
            } else {
              acc
            }
          } else {
            acc ++ Map(inc._1 -> inc._2)
          }
      }.map { case (vMd5, (v, tm)) =>
        (vMd5, v, tm)
      }


      (res.map(_._2).toSeq, res.map(_._1).toSeq, res.map(_._3).toSeq)
    }
  }


  def bakData(typeName: String): Unit = {
    sql(s"""
       |select
       |  owner_data,
       |  owner_data_md5,
       |  ext_data,
       |  ext_data_md5,
       |  ext_data_tm
       |from $targetTable
       |where day = '$lastPar' and type = '$typeName'
      """.stripMargin).createOrReplaceTempView(s"${typeName}_tmp_tbl")
  }


  def insertSqlDay(typeList: Seq[String], currentPar: String): Unit = {
    val fullDF = typeList.map(typeName =>
      sql(s"""
       |select
       |  owner_data,
       |  owner_data_md5,
       |  ext_data,
       |  ext_data_md5,
       |  ext_data_tm,
       |  type
       |from full_ext_merge
       |where type = '$typeName'
      """.stripMargin)).reduce(_.union(_))
    fullDF.repartition(FnHelper.getCoalesceNum(fullDF.count())).createOrReplaceTempView("full_tbl_fnl")

    sql(
      s"""
        |insert overwrite table $targetTable
        |  partition(day, type)
        |select
        |  owner_data,
        |  owner_data_md5,
        |  ext_data,
        |  ext_data_md5,
        |  ext_data_tm,
        |  '${currentPar}' day,
        |  type
        |from full_tbl_fnl
      """.stripMargin)
  }
}
