package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.commons.utils.{DateUtils, PropUtils}
import com.mob.dataengine.core.utils.DataengineException
import com.mob.dataengine.engine.core.business.dpi.been.DPIParam
import com.mob.dataengine.engine.core.jobsparam.JobContext2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.spark_project.guava.io.Files

import scala.util.{Failure, Success, Try}

/**
 * 读取文件转成DataFrame
 */
case class InputHandler() extends Handler {

  override def handle(ctx: JobContext2[DPIParam]): Unit = {
    import ctx.param._
    import org.apache.spark.sql.functions._

    val schema = StructType(ctx.spark.table(PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_INPUT)
      .schema.filter(_.name != "version"))

    /* txt文件格式需要单独处理 */
    val df = readFromUrl(ctx, Some(schema))
    val df2 = Files.getFileExtension(value) match {
      case "txt" if sep.isEmpty =>
        throw new DataengineException(s"ERROR: input[value]:$value,input[sep]:$sep 参数错误,txt文件需要传入分隔符")
      case "txt" =>
        import ctx.spark.implicits._
        import org.apache.spark.sql.functions._
        df.select(explode_outer(split($"value", sep.get)))
          .toDF(schema.fieldNames: _*)
      case _ => df
    }
    df2.withColumn("date", lit(DateUtils.currentDay()))
        .createOrReplaceTempView("dpi_input_pre")
    ctx.sql(
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_INPUT}
         |       PARTITION (version = '${ctx.param.version}')
         |SELECT ${schema.fieldNames.map(fieldNames => s"nvl($fieldNames, '') as $fieldNames").mkString(",")}
         |FROM dpi_input_pre
         |""".stripMargin)
    ctx.spark.table(PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_INPUT)
      .where(s"version = '${ctx.param.version}'")
      .createOrReplaceTempView("dpi_input")
  }


  /**
   * 目前只支持txt,csv,xlsx,xls 4种格式,
   * txt不支持schema推断
   */
  def readFromUrl(ctx: JobContext2[DPIParam], schema: Option[StructType] = None): DataFrame = {
    import ctx.param._
    val options = new scala.collection.mutable.HashMap[String, String]
    options.put("header", "false")
    options.put("inferSchema", "true")

    val (_schema, source) = Files.getFileExtension(value) match {
      case "txt" =>
        (None, "text")
      case "csv" => (schema, "csv")
      // TODO: 存在依赖问题，待解决
      case "xls" | "xlsx" => (schema, "com.crealytics.spark.excel")
      case _ => throw new DataengineException(s"ERROR: input[value]:$value 参数错误,只支持txt和csv")
    }

    val reader = ctx.spark.read.format(source).options(options)
    Try(if (_schema.isDefined) {
      reader.schema(schema.get).load(url)
    } else {
      reader.load(url)
    }) match {
      case Success(value) => value
      case Failure(ex) =>
        ex.printStackTrace()
        throw new DataengineException(s"ERROR: input[value]:$url 地址无效")
    }
  }

}
