package com.mob.dataengine.utils.idmapping.hive

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}

import com.mob.dataengine.commons.traits.TableTrait
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

abstract class IdMappingSecBase(@transient val spark: SparkSession, day: String, idType: String,
                             test                   : Boolean = false) extends Serializable with TableTrait {
  val srcTable: String = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC_INC
  val ieidTransformTable: String = PropUtils.HIVE_TABLE_DIM_IEID_TRANSFORM_FULL_PAR_SEC
  val ifidTransformTable: String = PropUtils.HIVE_TABLE_DIM_IFID_TRANSFORM_FULL_PAR_SEC

  val targetTable: String = idType match {
    case "ieid" => PropUtils.HIVE_TABLE_DM_IEID_MAPPING
    case "ifid" => PropUtils.HIVE_TABLE_DM_IFID_MAPPING
    case "snid" => PropUtils.HIVE_TABLE_DM_SNID_MAPPING
    case "mcid" => PropUtils.HIVE_TABLE_DM_MCID_MAPPING
    case "pid" => PropUtils.HIVE_TABLE_DM_PID_MAPPING
    case "oiid" => PropUtils.HIVE_TABLE_DM_OIID_MAPPING
    case _ => PropUtils.HIVE_TABLE_DM_ISID_MAPPING
  }

  val allFields: Array[String] = spark.table(targetTable).schema.fields.map(_.name)
  // 去掉分区字段,主键字段和主键_md5
  val fields: Array[String] = allFields
    .filter(f => !(f.equals("day") || f.equals("plat")))
    .filter(f => !(f.equals("device") || f.equals("device_tm") || f.equals("device_ltm") || f.equals("duid")))

  val tmFields: Array[String] = fields.filter(_.endsWith("_tm")).map(f => f.substring(0, f.length - 3))
  // mcid,mcid_md5,mcid_tm,mcid_ltm按顺序排列
  val fieldsWithTm: Array[String] = tmFields.flatMap(f => Seq(f, s"${f}_tm", s"${f}_ltm"))
  // _tm 字段
  val idTypeTm = s"${if (idType.contains("ieid")) "ieid" else idType}_tm"
  // _ltm 字段
  val idTypeLtm = s"${if (idType.contains("ieid")) "ieid" else idType}_ltm"
  val isIeidTable: Boolean = idType.equals("ieid")
  val isIfidTable: Boolean = idType.equals("ifid")
//  val md5Fields: Array[String] = fields.filter(_.contains("_md5"))
  val nonMd5Fields: Array[String] = Array("device", "snid") ++ tmFields

  val otherIdCols: Seq[String] = tmFields.diff(Seq(idType, "snid"))

  def prepare(): Unit = {
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }

  val fieldsQuery: String = if (tmFields.nonEmpty) {
    tmFields.map{ x =>
      s"get_latest_elements($x, ${x}_tm, ${x}_ltm, 200) as ${x}_struct"
    }.mkString(",") + ","
  } else {
    ""
  }

  val t1Columns: Seq[String] = Seq(s"$idType", "device", "device_tm", "device_ltm", "duid", "plat") ++ fieldsWithTm
  val t1Schema: StructType = StructType(
    Seq(StructField(s"$idType", StringType, nullable = true),
      StructField("device", StringType, nullable = true),
      StructField("device_tm", StringType, nullable = true),
      StructField("device_ltm", StringType, nullable = true),
      StructField("duid", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("plat", StringType, nullable = true)) ++
      fieldsWithTm.map(f => StructField(f, ArrayType(StringType, containsNull = true), nullable = true)))

  val aggClause: String = if (tmFields.nonEmpty) {
    (tmFields).map(f => s"aggregateDevice(concat_ws(',', $f), concat_ws(',', ${f}_tm)," +
      s" concat_ws(',', ${f}_ltm)) as agg_$f").mkString(",") + ","
  } else {
    ""
  }

  val collapseClause: String = (Seq("device") ++ tmFields).map{ f =>
    if (nonMd5Fields.toSet.contains(f)) {
      s"$f, ${f}_tm, ${f}_ltm"
    } else {
      s"collapse_array($f, ${f}_tm, ${f}_ltm, ${f}_md5, ${f}_md5_tm) as collapsed_$f"
    }
  }.mkString(",")

  // 搭配上面的collapseClause, 从其中取出数据
  val selectFromCollapsedCols: String = (Seq("device") ++ tmFields).map{ f =>
    if (nonMd5Fields.toSet.contains(f)) {
      s"$f, ${f}_tm, ${f}_ltm"
    } else {
      s"collapsed_$f._1 as $f, collapsed_$f._2 as ${f}_md5, collapsed_$f._3 as ${f}_tm, collapsed_$f._4 as ${f}_ltm"
    }
  }.mkString(",")


  // 最后将所有为空对数据类型置为null
  val empty2NullClause: String = spark.table(targetTable).schema.fields
    .filter(_.dataType.isInstanceOf[ArrayType])
    .map(_.name).filter(!_.equals("device_plat"))
    .map(f => s"if($f is null or size($f) < 1, null, $f) $f")
    .mkString(",")

  // 全量和增量合并的sql语句
  // 全量表表名: full_tb, 增量表表名: incr_tb
  val combineIncrClause: String = (Seq("device") ++ tmFields).map{ f =>
    if (nonMd5Fields.toSet.contains(f)) {
      s"""
         |combine_incr(full_tb.$f, null, full_tb.${f}_tm, full_tb.${f}_ltm,
         |  incr_tb.$f, null, incr_tb.${f}_tm, incr_tb.${f}_ltm
         |) as combined_$f
       """.stripMargin
    } else {
      s"""
         |combine_incr(full_tb.$f, full_tb.${f}_md5, full_tb.${f}_tm, full_tb.${f}_ltm,
         |  incr_tb.$f, incr_tb.${f}_md5, incr_tb.${f}_tm, incr_tb.${f}_ltm
         |) as combined_$f
       """.stripMargin
    }
  }.mkString(",")

  // 搭配上面的combineIncrClause, 从其中取出数据
  val selectFromCombinedCols: String = (Seq("device") ++ tmFields).map{ f =>
    if (nonMd5Fields.toSet.contains(f)) {
      s"combined_$f._1 as $f, combined_$f._3 as ${f}_tm, combined_$f._4 as ${f}_ltm"
    } else {
      s"combined_$f._1 as $f, combined_$f._2 as ${f}_md5, combined_$f._3 as ${f}_tm, combined_$f._4 as ${f}_ltm"
    }
  }.mkString(",")

  val beforeDay: String = DateUtils.format(DateUtils.getDayTimeStamp(day, -1).toLong * 1000)

  def unionIeid14Clause(srcDF: DataFrame): DataFrame = {
    srcDF.join(spark.table(ieidTransformTable).select("ieid", "ieid14_lower")
      .where("ieid14_lower != ieid15_lower").distinct(),
      Seq("ieid")
    ).drop("ieid").withColumnRenamed("ieid14_lower", "ieid")
  }

  // 创建Source tab，读取device_mapping_v3 增量数据
  def createSourceTmpTable(): DataFrame = {
    val datasetRaw: DataFrame = sql(
      s"""
         |select
         |  device,
         |  $idType,
         |  $idTypeTm,
         |  $idTypeLtm,
         |  $fieldsQuery
         |  get_max_elements(duid, 200) as duid,
         |  plat
         |from $srcTable
         |  where
         |    day='$day' and $idType is not null and size($idType)>0
         |    and $idTypeTm is not null and size($idTypeTm)>0
         |    and $idTypeLtm is not null and size($idTypeLtm)>0
      """.stripMargin
    )

    // 测试环境取部分数据
    val dataset: DataFrame = if (test) {
      datasetRaw.filter(r => {
        val a = r.getString(0).charAt(39)
        // sample
        a == '0' || a == '6' || a == 'b'
      })
    } else datasetRaw

    val tmpDF = dataset.flatMap { r =>
      val device = r.getAs[String]("device")
      val ids = r.getAs[Seq[String]](s"$idType")
      val idsTm = r.getAs[Seq[String]](s"$idTypeTm")
      val idsLtm = r.getAs[Seq[String]](s"$idTypeLtm")

      val structs = tmFields.map(f => r.getAs[Row](s"${f}_struct"))

      val duid = r.getAs[Seq[String]]("duid")
      val plat = r.getAs[String]("plat")

      try {
        (0 until Math.min(Math.min(ids.size, idsTm.size), idsLtm.size)).indices.map(i => {
          val tmp = Seq(ids(i), device, idsTm(i), idsLtm(i), duid, plat) ++
            structs.flatMap(s => Seq(s.getAs[Seq[String]]("_1"),
              s.getAs[Seq[String]]("_2"), s.getAs[Seq[String]]("_3")))
          Row.fromSeq(tmp)
        })
      } catch {
        case e: Exception =>
          println(s"$device\t$ids\t$idsTm\t$idsLtm\t$plat")
          e.printStackTrace()
          throw e;
      }
    }(RowEncoder(t1Schema)).toDF(t1Columns: _*)
    if (isIeidTable) {
      tmpDF.unionByName(unionIeid14Clause(tmpDF))
    } else {
      tmpDF
    }
  }

  val transformSql: String
  val insertSql: String

  def run(): Unit = {
    val d1 = createSourceTmpTable()
    // val d2 = buildExtMappingTable() 不再合并外部数据
    d1.createOrReplaceTempView("t1")
    sql(transformSql).createOrReplaceTempView("joined_tb")
    sql(insertSql)
  }

  def stop(): Unit = {
    spark.stop()
  }

  def unionDF(d1: DataFrame, d2: DataFrame): DataFrame = {
    val schema1 = d1.schema.fieldNames
    val schema2 = d2.schema.fieldNames
    val fullSchema = schema1.union(schema2)
    d1.selectExpr(fullSchema.map(f => if (schema1.toSet.contains(f)) f else s"null as $f"): _*)
      .union(d2.selectExpr(fullSchema.map(f => if (schema2.toSet.contains(f)) f else s"null as $f"): _*))
  }

  def buildExtMappingTable(): DataFrame = {
    val df = sql(
      s"""
         |select owner_data, ext_data, ext_data_tm, type
         |from ${PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL_INC_VIEW}
         |where type rlike 'ieid'
         |union all
         |select owner_data, ext_data, ext_data_tm, type
         |from ${PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL_INC_VIEW}
         |where type not rlike 'ieid'
       """.stripMargin)
      .toDF()
      .filter(r => r.getAs[String]("type").startsWith(idType))
      .withColumnRenamed("owner_data", idType)

    val expandColumns = otherIdCols.flatMap{ f =>
      Seq(
        (s"if (type='${idType}_$f', ext_data, cast(null as array<string>))", f),
        (s"if (type='${idType}_$f', ext_data_tm, cast(null as array<string>))", s"${f}_tm"),
        (s"if (type='${idType}_$f', ext_data_tm, cast(null as array<string>))", s"${f}_ltm")
      )
    }

    df.selectExpr(Seq(idType) ++ expandColumns.map(_._1): _*)
      .toDF(Seq(idType) ++ expandColumns.map(_._2): _*)
  }
}
