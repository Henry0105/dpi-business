package com.mob.dataengine.engine.core.profilecal.helper

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

import com.mob.dataengine.commons.profile.{IndividualProfile, MetadataUtils}
import com.mob.dataengine.engine.core.jobsparam.profilecal.ProfileBatchMonomerParam
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProfileBatchMonomerJobHelper {

  val num2str: String = "num2str"
  val kvSep: String = "\u0001"
  val pairSep: String = "\u0002"
  val pSep: String = "\u0003"
  val viewSuffix: String = "_view"

  def registerNum2Str(spark: SparkSession): Unit = {
    // 注册一个处理科学计数法问题的udf
    val df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
    df.setMaximumFractionDigits(340) // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS
    val dfBC = spark.sparkContext.broadcast(df)
    spark.udf.register(num2str, (d: Double) => {
      dfBC.value.format(d)
    })
  }

  /**
   * sql e.g.
   * select pid
   * , concat_ws('$pairSep',
   * concat_ws('$kvSep', '8122_1000', cast(student as string)),
   * concat_ws('$kvSep', '8123_1000', cast(student as string))
   * )
   * from rp_mobdi_app.mobfin_pid_profile_lable_full_par
   */
  def handleTags(spark: SparkSession, param: ProfileBatchMonomerParam, confidenceIds: Seq[String],
                 whereExprs: Option[String] = None): Array[DataFrame] = {
    val profileMark = 0
    val confidenceMark = 1

    val profileInfos = MetadataUtils.findIndividualProfile(param.profileIds, whereExprs)
    val confidenceInfos = MetadataUtils.findProfileConfidence(param.profileIds.intersect(confidenceIds), whereExprs)
    val allInfos = profileInfos.map((profileMark, _))
      .union(confidenceInfos.map((confidenceMark, _)))
      .map { case (mark, info) => (mark, info.copy(table = info.table.concat(viewSuffix))) }

    println(s"profileInfos nums:${profileInfos.length}, confidenceInfos nums:${confidenceInfos.length}")
    allInfos.groupBy(_._2.dbTable).toParArray.map { case (_, xs) =>
      val (confidenceXs, tagsXs) = xs.partition(_._1 == confidenceMark)
      val confidenceCol = genConcatCol(confidenceXs.map(_._2))
      val tagsCol = genConcatCol(tagsXs.map(_._2))

      spark.sql(
        s"""
           |select pid
           |     , $tagsCol as tags
           |     , $confidenceCol as confidence
           |from ${xs.head._2.dbTable}
           |""".stripMargin)
    }.toArray
  }

  def genConcatCol(xs: Seq[IndividualProfile]): String = {
    val ys = xs.map { x =>
      s"concat_ws('$kvSep', '${x.fullId}', ${valueToStr(x.valueColType, x.valueColName)})"
    }
    if (ys.isEmpty) {
      "null"
    } else {
      s"concat_ws('$pairSep', ${ys.mkString(", ")})"
    }
  }

  // -------------------------------   udf   -------------------------------
  def valueToStr(dataType: String, value: String): String = {
    dataType match {
      case "string" => s"$value"
      case "double" => s"$num2str($value)"
      case _ => s"cast($value as string)"
    }
  }

  def kvStr2map(str: String): Map[String, String] = {
    if (str == null) {
      return null
    }
    val ms = str.split(pairSep)
      .map(_.split(kvSep))
      .withFilter(kv => kv.length == 2)
      .map { kv => kv(0) -> kv(1) }
      .toMap
    if (ms.isEmpty) null else ms
  }

  def kvSeq2map(xs: Seq[String]): Map[String, String] = {
    if (xs == null) {
      return null
    }
    val ms = xs.flatMap { x =>
      x.split(pairSep)
        .map(_.split(kvSep))
        .withFilter(kv => kv.length == 2)
        .map { kv => kv(0) -> kv(1) }
    }.toMap
    if (ms.isEmpty) null else ms
  }

}
