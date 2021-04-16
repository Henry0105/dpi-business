package com.mob.dataengine.engine.core.mapping

import com.mob.dataengine.commons.DeviceSrcReader
import com.mob.dataengine.commons.annotation.code.author
import com.mob.dataengine.commons.enums.InputType
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.{BaseJob2, JobContext2, PidOperatorMappingParam}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * 号码归属地基本任务：
 * 1、build input DF 从(Cache/Cache_New/Hub)获取种子数据
 * 2、build mapping DF 获取mapping表数据
 * 3、build join DF 获取种子数据对应mapping中的数据
 * 4、build fliter DF 根据参数指定的省市/运行商进行数据过滤
 * 5、persist fliter DF to Cache_New(toHive)
 * 6、build hubseed DF 从Hub表中获取种子数据
 * 7、persist（fliter DF left join hubseed DF) as feature DF to HubTable
 */

@author("dlzhang")
object PidOperatorMapping extends BaseJob2[PidOperatorMappingParam] {

  override def cacheInput: Boolean = true

  /**
   * 生成结果DF
   *
   * @param df  transform dataframe
   * @param ctx 任务上下文
   * @return
   */
  override def transformData(df: DataFrame, ctx: JobContext2[PidOperatorMappingParam]): DataFrame = {
    val srcDF = buildInputDataFrame(ctx)
    val desDF = buildInputMappingDataFrame(ctx)
    val resJoinDF = joinedDF(srcDF, desDF, ctx)
    val fliterDF = filterConditionDF(resJoinDF, ctx)
    fliterDF
  }

  /**
   * 获得种子数据
   *
   * @param ctx 任务执行上下文
   * @return
   */
  override def buildInputDataFrame(ctx: JobContext2[PidOperatorMappingParam]): DataFrame = {
    import ctx.{param, spark}
    val srcDF = param.inputs.map(input => {
      DeviceSrcReader.toDFV2(spark, input, hubService = ctx.hubService).select("id", "data")
    }).reduce(_ union _)

    // 检测数据元结构是否有title并去除
    if (InputType.isDfs(param.inputTypeEnum) && param.sep.isDefined) {
      // 这里header一定是有的,python端会做处理将源文件中的header去掉,参数里面传header进来
      // 并带上 `in_` 这样的前缀
      require(param.idx.nonEmpty)
      require(param.header > 0)
      require(param.headers.nonEmpty)
      require(param.headers.size >= param.idx.get)

      srcDF.filter(s"data != '${param.headers.get.map(_.substring(3)).mkString(param.sep.get)}'")
    } else {
      srcDF
    }
  }

  /**
   * 获得输入Mapping数据
   */
  def buildInputMappingDataFrame(ctx: JobContext2[PidOperatorMappingParam]): DataFrame = {
    import ctx.spark

    ctx.spark.udf.register("carrier2code", carrierToCode _)
    ctx.spark.udf.register("clean_city", cleanCity _)

    val areaParDay = ctx.getLastPar(PropUtils.HIVE_TABLE_MAPPING_AREA_PAR,
      0, s => s.matches("flag=\\d{8,8}")).split("=")(1)


    // 出于兼容性的考虑将表做成和老表一样的结构 rp_marketplus_wisdom.call_pid_property
    ctx.sql(
      s"""
         |select pid_id pid, carrier isp, carrier2code(carrier) isp_code, b.city_code, b.city city_name, b.province_code
         |from ${PropUtils.HIVE_TABLE_DIM_PID_ATTRIBUTE_FULL_PAR_SEC} as a
         |join (
         |  select city, city_code, province_code
         |  from ${PropUtils.HIVE_TABLE_MAPPING_AREA_PAR}
         |  where flag='$areaParDay' and length(city) > 0
         |  group by city, city_code, province_code
         |) as b
         |on clean_city(a.city) = b.city
       """.stripMargin).distinct()
  }

  // 根据运营商的中文转成对应id
  def carrierToCode(carrier: String): String = {
    if (StringUtils.isNotBlank(carrier)) {
      carrier match {
        case "移动" => "1"
        case "联通" => "2"
        case "电信" => "3"
        case _ => "4"
      }
    } else {
      "4"
    }
  }

  // 将城市中的 "市" 这个字段去掉
  // todo 这里为了join需要对city字段做处理
  def cleanCity(city: String): String = {
    city match {
      case null => null
      case "吉林市" => "吉林市"
      case "那曲地区" => "那曲"
      case x if x.endsWith("市") => city.substring(0, city.length - 1)
      case s => s
    }
  }

  /**
   * 获取join数据
   *
   * @param srcDF
   * @param desDF
   * @param ctx
   * @return
   */
  def joinedDF(srcDF: DataFrame, desDF: DataFrame, ctx: JobContext2[PidOperatorMappingParam]): DataFrame = {
    import ctx.spark
    srcDF.createOrReplaceTempView("seed_pid_table")
    desDF.createOrReplaceTempView("isp_pid_table")

    // 根据pid的前7位进行join,获取运营商等信息
    val resJoinDF = spark.sql(
      s"""
         |select a.id, data,isp, city_code, city_name,
         |  case when isp_code != 'null' then isp_code else '4' end as isp_code, province_code
         |from (
         |  select
         |   id,data
         |  from seed_pid_table
         |) a
         |left join (
         |  select  pid, isp, isp_code, city_code, city_name, province_code
         |  from isp_pid_table
         |) b
         |on a.id = b.pid
         |group by a.id,data,isp,isp_code,city_code,city_name, province_code
       """.stripMargin)
    resJoinDF
  }

  /** 根据参数的include和exclude字段进行二维的筛选 */
  def filterConditionDF(srcDF: DataFrame, ctx: JobContext2[PidOperatorMappingParam]): DataFrame = {
    import ctx.param
    // include字段存在
    val filterDF1 = if (param.include != null && param.include.nonEmpty) {
      val cityCode: String = param.include.getOrElse("city_code", "")
      val ispCode: String = param.include.getOrElse("isp_code", "")
      val provCode: String = param.include.getOrElse("province_code", "")
      filterByInclude(srcDF, cityCode, ispCode, provCode)
    } else {
      srcDF
    }

    /** exclude字段存在 */
    val filterDF2 = if (param.exclude != null && param.exclude.nonEmpty) {
      val city_code: String = param.exclude.getOrElse("city_code", "")
      val isp_code: String = param.exclude.getOrElse("isp_code", "")
      val provCode: String = param.exclude.getOrElse("province_code", "")
      filterByExclude(filterDF1, city_code, isp_code, provCode)
    } else {
      filterDF1
    }
    filterDF2
  }

  def includeValues(df: DataFrame, field: String, _values: Array[String]): DataFrame = {
    val values = _values.filter(_.nonEmpty)
    if (values.isEmpty) {
      df
    } else {
      val strValues = values.map(v => s"'$v'")
      df.filter(s"$field in (${strValues.mkString(",")})")
    }
  }

  def excludeValues(df: DataFrame, field: String, _values: Array[String]): DataFrame = {
    val values = _values.filter(_.nonEmpty)
    if (values.isEmpty) {
      df
    } else {
      val strValues = values.map(v => s"'$v'")
      df.filter(s"$field not in (${strValues.mkString(",")})")
    }
  }

  // 筛选include字段
  def filterByInclude(df: DataFrame, cityCode: String, ispCode: String, provCode: String): DataFrame = {
    df.transform(includeValues(_: DataFrame, "city_code", cityCode.split(",")))
      .transform(includeValues(_: DataFrame, "isp_code", ispCode.split(",")))
      .transform(includeValues(_: DataFrame, "province_code", provCode.split(",")))
  }

  // 剔除exclude字段
  def filterByExclude(df: DataFrame, cityCode: String, ispCode: String, provCode: String): DataFrame = {
    df.transform(excludeValues(_: DataFrame, "city_code", cityCode.split(",")))
      .transform(excludeValues(_: DataFrame, "isp_code", ispCode.split(",")))
      .transform(excludeValues(_: DataFrame, "province_code", provCode.split(",")))
  }

  /**
   * 输出数据到Cache_New
   *
   * @param fliterDF 持久化到hive的dataframe
   * @param ctx      任务上下文
   */
  override def persist2Hive(fliterDF: DataFrame, ctx: JobContext2[PidOperatorMappingParam]): Unit = {
    import ctx.{param, spark}
    fliterDF.createTempView("srcView")

    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |partition(uuid='${param.output.uuid}', day='${ctx.jobCommon.day}')
         |select
         |  id,
         |  map(3, id, 10, isp, 11, isp_code, 12, city_code, 13, city_name, 14, province_code) match_ids,
         |  3 as id_type,
         |  0 as encrypt_type,
         |  data
         |from
         |  srcView
       """.stripMargin)

    persist2DataHub(fliterDF, ctx);
  }

  /**
   * 输出数据到Hub
   *
   * @param fliterDF
   * @param ctx
   */
  def persist2DataHub(fliterDF: DataFrame, ctx: JobContext2[PidOperatorMappingParam]): Unit = {
    import ctx.{hubService, param}
    val mergeFeatureUDF = udf(mergeFeature(_: Map[String, Seq[String]], _: Map[String, Seq[String]]))
//    ctx.spark.udf.register("mergeFeature", mergeFeature(_: Map[String, Seq[String]], _: Map[String, Seq[String]]))

    val resultDF = fliterDF.select(map(
      lit("seed"), array(col("data")),
      lit("3"), array(col("id")
        , col("isp")
        , col("isp_code")
        , col("city_code")
        , col("city_name")
        , col("province_code"))
    ).as("feature"),
      col("id")
    )

    // 从Hub获取种子数据
    // ctx.spark.conf.set("spark.sql.broadcastTimeout", 1200)
    ctx.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val seedDF = readFromSeedHub(ctx)
    val featureDF = resultDF.join(seedDF, resultDF("id") === seedDF("seed"), "left")
      .select(mergeFeatureUDF(resultDF("feature"), seedDF("feature")).as("feature"))
    hubService.writeToHub(featureDF, param.output.uuid)
  }

  /**
   * Feature字段聚合，方法实现
   *
   * @param resultMap
   * @param seedMap
   * @return
   */
  def mergeFeature(resultMap: Map[String, Seq[String]], seedMap: Map[String, Seq[String]]): Map[String, Seq[String]] = {
    val tmpRes =
      if (null == resultMap || resultMap.isEmpty) {
        seedMap
      } else if (null == seedMap || seedMap.isEmpty) {
        resultMap
      } else {
        seedMap ++ resultMap
      }
    tmpRes.filter { case (_, seq) => null != seq && seq.nonEmpty }
  }

  /**
   * 获取 hubservice 数据源数据
   *
   * @param ctx
   * @return
   */
  def readFromSeedHub(ctx: JobContext2[PidOperatorMappingParam]): DataFrame = {
    import ctx.spark.implicits._
    ctx.param.inputs.map { input =>
      ctx.sql(
        s"""
           |select feature
           |from ${PropUtils.HIVE_TABLE_DATA_HUB}
           |where uuid = '${input.uuid}'
          """.stripMargin)
    }.reduce(_ union _)
      .select($"feature".getField("seed").getItem(0).as("seed"), $"feature")
      .withColumn("rn", row_number().over(Window.partitionBy($"seed").orderBy($"seed")))
      .where($"rn" === 1)
      .select("feature", "seed")
  }

  /**
   * @param df  输入DF
   * @param ctx 任务上下文
   **/
  override def transformSeedDF(df: DataFrame, ctx: JobContext2[PidOperatorMappingParam]): DataFrame = {
    var dataFrame = df
    println("ctx.param.output.keepSeed" + ctx.param.output.keepSeed)
    if (ctx.param.output.keepSeed != 1) {
      dataFrame = df.select(s"${ctx.param.fieldName}")
    }
    dataFrame
  }


  /**
   * @param ctx 任务上下文
   * @return
   */
  override def createMatchIdsCol(ctx: JobContext2[PidOperatorMappingParam]): Seq[Column] = Nil
}


