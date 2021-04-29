package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.business.dpi.DpiMktUrl.query
import com.mob.dataengine.engine.core.business.dpi.been.{DPIParam, UrlStruct}
import com.mob.dataengine.engine.core.jobsparam.JobContext2
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}

case class GenTagHandler() extends Handler {

  /**
   * 步骤四: 生成tag
   * 步骤五: 整合
   * table transform: dpi_tb3 => dm_dpi_mapping_test.dm_dpi_mkt_url_tag_v2 => dm_dpi_mapping_test.dpi_mkt_url_withtag
   */
  override def handle(ctx: JobContext2[DPIParam]): Unit = {
    import ctx.param
    ctx.spark.udf.register("set_equals", arrayEquals _)
    ctx.spark.conf.set("spark.sql.crossJoin.enabled", "true")

    // 中柱模式
    val mode1List = param.carrierInfos
      .withFilter(info => param.carriers.contains(info.name) && info.genType == 0).map(_.name).sorted

    // 浩智模式
    val mode2List = param.carrierInfos
      .withFilter(info => param.carriers.contains(info.name) && info.genType == 1).map(_.name).sorted


    // 模式一

    // 生成tag的动作
    param.carrierInfos.withFilter(info => StringUtils.isNotBlank(info.genTagSql))
      .withFilter(info => mode1List.contains(info.name)).foreach {
      info =>
        println(info.name)
        // PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG_V2
        query(ctx, info.genTagSql, Some(Map("version" -> param.version, "carrier" -> info.name,
          param.srcTable -> "dpi_tb3",
          s"${param.targetTable}_1" -> PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG,
          s"${param.targetTable}_2" -> PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG
        )))
        // 提示功能:url是否已经存在
        reportUrlRepetition(ctx, info.name)
    }


    // 模式二

    // 生成tag的动作
    if  (mode2List.nonEmpty) {

      val head = mode2List.head
      val tail = mode2List.tail
      param.carrierInfos.withFilter(info => StringUtils.isNotBlank(info.genTagSql))
        .withFilter(info => Set(head).contains(info.name)).foreach {
        info =>
          println(info.name)
          // PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG_V2
          query(ctx, info.genTagSql, Some(Map("version" -> param.version, "carrier" -> info.name,
            param.srcTable -> "dpi_tb3",
            s"${param.targetTable}_1" -> PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG,
            s"${param.targetTable}_2" -> PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG
          )))
          // 提示功能:url是否已经存在
          reportUrlRepetition(ctx, info.name)
      }

      if (tail.nonEmpty) {
        // 生成tag的动作
        param.carrierInfos.withFilter(info => StringUtils.isNotBlank(info.genTagSql))
          .withFilter(info => tail.contains(info.name)).foreach {
          info =>
            println(info.name)
            // PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG_V2
            query(ctx,
              s"""
                 |insert overwrite table ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG}
                 |partition(version='${param.version}' and carrier='${info.name}')
                 |select
                 |${fieldNamesNoPart(ctx.spark, PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG, 2)}
                 |from ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG}
                 |where version='${param.version}' and carrier='${head}';
                 |insert overwrite table ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG}
                 |partition(version='${param.version}' and carrier='${info.name}')
                 |select
                 |${fieldNamesNoPart(ctx.spark, PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG, 2)}
                 |from ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG}
                 |where version='${param.version}' and carrier='${head}';
                 |""".stripMargin, None)
            // 提示功能:url是否已经存在
            reportUrlRepetition(ctx, info.name)
        }
      }
    }
    // 解锁
    param.jdbcTools.executeUpdateWithoutCheck(
      s"UPDATE dpi_job_lock SET locked=0,update_time='${DateUtils.getNowTT()}' WHERE version='${ctx.param.version}'")
  }


  def fieldNamesNoPart(spark: SparkSession, table: String, numPart: Int): String = {
    val schema = spark.table(PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG).schema
    (0 until numPart).foreach(i => {
      schema.init
    })
    schema.map(_.name).mkString(",")
  }

  /**
   * new         old        结果(是否一致)
   * 1.  [a, b, c]    [a, b, c]         Y
   * 2.  [a, b, c]    [a, b, d]         N
   * 3.  [a, b, c]    [a, b]            Y
   * 4.  [a, b]       [a, b, c]         N
   * old是new的子集即一致
   */
  def arrayEquals(newArr: Seq[Row], oldArr: Seq[Row]): String = {
    if (oldArr.size > newArr.size) {
      return null
    }

    def trans2Structs(arr: Seq[Row]): Set[UrlStruct] = {
      arr.map {
        r =>
          val tag = r.getAs[String]("tag")
          val url = r.getAs[String]("url")
          val urlRegexp = r.getAs[String]("url_regexp")
          val urlKey = r.getAs[String]("url_key")
          UrlStruct(tag, url, urlKey, urlRegexp)
      }.toSet
    }

    val newSet = trans2Structs(newArr)
    val oldSet = trans2Structs(oldArr)
    val inter = newSet.intersect(oldSet)
    if (oldSet.size == inter.size) s"${newSet.head.tag},${oldSet.head.tag}" else null
  }

  /** 判断url是否重复，如果重复，将具体的tag写入回调信息 */
  def reportUrlRepetition(ctx: JobContext2[DPIParam], carrier: String): Unit = {
    val df = ctx.sql(
      s"""
         |select tag, collect_set(struct(tag, url, url_regexp, url_key)) as new_url_set
         |from ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG}
         |where carrier = '$carrier' and version = '${ctx.param.version}'
         |group by tag
         |""".stripMargin).cache()
    df.createOrReplaceTempView("rur_1")

    ctx.sql(
      s"""
         |select tag, collect_set(struct(tag, url, url_regexp, url_key)) as old_url_set
         |from ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_TAG}
         |where carrier = '$carrier' and version <> '${ctx.param.version}'
         |group by tag
         |""".stripMargin).createOrReplaceTempView("rur_2")

    val repetitiveTags = ctx.sql(
      s"""
         |select repetitive_tags
         |from (
         |      select t1.tag, set_equals(new_url_set, old_url_set) as repetitive_tags
         |      from rur_1 t1
         |      cross join rur_2 t2
         |     ) as a
         |where repetitive_tags is not null
         |""".stripMargin).collect().map {
      r => r.getAs[String]("repetitive_tags")
    }

    ctx.param.cbBean.setRepetitiveTags(carrier, repetitiveTags)
  }

}
