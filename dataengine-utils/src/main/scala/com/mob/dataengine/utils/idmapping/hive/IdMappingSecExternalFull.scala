package com.mob.dataengine.utils.idmapping.hive

import java.util.regex.Pattern

import com.mob.dataengine.commons.traits.TableTrait
import com.mob.dataengine.commons.utils.{FnHelper, PropUtils}
import com.mob.dataengine.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class IdMappingSecExternalFull(@transient val spark: SparkSession, day: String, once: Option[Boolean] = Some(false),
                            test               : Boolean = false)
  extends Serializable with TableTrait {
  val srcTable: String = PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC
  val targetTable: String = PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL
  val targetIncTable: String = PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL_INC

  def prepare(): Unit = {
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
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

  // 将type字段清理成正常的,去掉aes,14,15这样的
  // 原type例如: ieid14_pidaes, pidaes_isid
  def cleanType(s: String): String = {
    s.replaceAll("md5|aes|14|15", "")
  }

  def createSourceTmpTable(): Unit = {
    // 这里判断是否去某一天的分区, 还是所有的历史分区
    val dayFilter = if (once.nonEmpty && once.get) s" day <= '$day'" else s" day = '$day'"

    sql(
      s"""
         |select stack(2,
         |  owner_data, ext_data, processtime, type,
         |  ext_data, owner_data, processtime, reverse_type(type)
         |) as (owner_data, ext_data, processtime, type)
         |from (
         |  select trim(owner_data) owner_data, trim(ext_data) ext_data, processtime,
         |    regexp_replace(type, 'md5|aes|14|15', '') type
         |  from $srcTable
         |  where $dayFilter
         |) as a
       """.stripMargin).toDF("owner_data", "ext_data", "processtime", "type")
      .createOrReplaceTempView("tmp_source")

    val datasetRaw: DataFrame = sql(
      s"""
         |select owner_data,
         |  collect_list(CASE WHEN ext_data IS NULL THEN '' ELSE ext_data END) as ext_data,
         |  collect_list(processtime) as ext_data_tm,
         |  type
         |from (
         |  select owner_data, ext_data, processtime, type,
         |    row_number() over (partition by owner_data,
         |    ext_data, type order by processtime asc) rn
         |  from tmp_source
         |) as a
         |where rn = 1
         |group by owner_data, type
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
    // 处理各个分区下的数据
    for (typeName <- typeList) {
      registerData(typeName)
    }
    createFullTbl(typeList)
    sql(transformSql).cache().createOrReplaceTempView("full_ext_merge")
    for (typeName <- typeList) {
      insertIntoIncrTable(typeName, toDay)
    }
    SparkUtils.createView(spark, targetIncTable, toDay, "day")
    insertIntoFullTable(typeList, currentPar)
    SparkUtils.createView(spark, targetTable, currentPar, "day")
  }

  def insertIntoIncrTable(typeName: String, beforeDay: String): Unit = {
    sql(
      s"""
         |insert overwrite table $targetIncTable
         |  partition(day, type)
         |select
         |  owner_data,
         |  ext_data,
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

  // 获取所有的idmapping的对应关系
  // 并做一个相反的出来
  def getTypeList(table: String, isSrc: Option[Boolean] = Some(false)): Array[String] = {
    val lastPars = sql(s"show partitions $table").collect().map(_.getAs[String](0))
    val types = lastPars.map(par => cleanType(par.split("\\/")(1).split("=")(1))).distinct
    if (types.length == 0) {
      Array.empty[String]
    } else {
      types.flatMap(tmpType => {
        if (isSrc.nonEmpty && isSrc.get) {
          Seq(tmpType,
            Pattern.compile("^([\\w'-]+)[_]+([\\w'-]+)", Pattern.MULTILINE)
              .matcher(tmpType).replaceAll("$2_$1") // 做个相反的出来
          )
        } else Seq(tmpType)
      }).distinct
    }
  }

  def createFullTbl(typeList: Seq[String]): Unit = {
    typeList.map(typeName =>
      sql(
        s"""
           |SELECT owner_data,
           |    ext_data,
           |    ext_data_tm,
           |    '$typeName' AS type
           |FROM ${typeName}_tmp_tbl
         """.stripMargin)).reduce(_.union(_)).createOrReplaceTempView("full_tbl")
  }

  val transformSql: String = {
    spark.udf.register("merge_inc2full", mergeIncr2Full _)

    s"""
       |select owner_data, zipped._1 ext_data, zipped._2 ext_data_tm, type
       |from (
       |  select
       |    coalesce(full_tbl.owner_data,ext_incr.owner_data) owner_data,
       |    merge_inc2full(full_tbl.ext_data, ext_incr.ext_data,
       |      full_tbl.ext_data_tm, trans_date(ext_incr.ext_data_tm)) zipped,
       |    coalesce(full_tbl.type,ext_incr.type) type
       |  from full_tbl
       |  full join ext_incr
       |  on full_tbl.owner_data = ext_incr.owner_data
       |    and coalesce(full_tbl.type,ext_incr.type) = ext_incr.type
       |) merged
      """.stripMargin
  }

  // 将增量表的数据合并入全量表
  def mergeIncr2Full(fullExtData: Seq[String], incExtData: Seq[String],
    fullTm: Seq[String], incTm: Seq[String]): (Seq[String], Seq[String]) = {
    if (fullExtData == null || fullExtData.isEmpty) {
      (incExtData, incTm)
    } else if (incExtData == null || incExtData.isEmpty) {
      (fullExtData, fullTm)
    } else { // 合并数据
      val fullZip = fullExtData.zip(fullTm).map{ case (v, tm) => (v, tm)}.toMap
      val incZip = incExtData.zip(incTm).map{ case (v, tm) => (v, tm)}.toMap

      val res = incZip.foldLeft(fullZip) { case (acc, inc) =>
          if (acc.contains(inc._1)) {  // 更新对应的时间戳
            if (acc(inc._1) > inc._2) {
              acc ++ Map(inc._1 -> inc._2)
            } else {
              acc
            }
          } else {
            acc ++ Map(inc._1 -> inc._2)
          }
      }.toSeq

      (res.map(_._1), res.map(_._2))
    }
  }


  def registerData(typeName: String): Unit = {
    sql(s"""
       |select
       |  owner_data,
       |  ext_data,
       |  ext_data_tm
       |from $targetTable
       |where day = '$lastPar' and type = '$typeName'
      """.stripMargin).createOrReplaceTempView(s"${typeName}_tmp_tbl")
  }


  def insertIntoFullTable(typeList: Seq[String], currentPar: String): Unit = {
    val fullDF = typeList.map(typeName =>
      sql(s"""
       |select
       |  owner_data,
       |  ext_data,
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
        |  ext_data,
        |  ext_data_tm,
        |  '$currentPar' day,
        |  type
        |from full_tbl_fnl
      """.stripMargin)
  }
}
