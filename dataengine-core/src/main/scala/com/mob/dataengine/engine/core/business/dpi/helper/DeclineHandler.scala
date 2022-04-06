package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.commons.DPIJobCommon
import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.business.dpi.DpiMktUrl.query
import com.mob.dataengine.engine.core.business.dpi.been.DPIParam
import com.mob.dataengine.engine.core.jobsparam.JobContext2
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SaveMode

case class DeclineHandler() extends Handler {

  /**
   * 步骤六: 初筛规则生成  dm_dpi_mapping_test.dpi_mkt_url_withtag => dm_dpi_mapping_test.dpi_mkt_url_first_filter_all_v3
   * 步骤七: mp文件      dm_dpi_mapping_test.dpi_mkt_url_withtag => dm_dpi_mapping_test.dpi_mkt_url_matcher_pattern
   */
  override def handle(ctx: JobContext2[DPIParam]): Unit = {
    import ctx.param

    param.carrierInfos.withFilter(info => StringUtils.isNotBlank(info.preScreenSql))
      .withFilter(info => param.carriers.contains(info.name)).foreach {
      info =>

        val _srcTable = if (info.genType == 1) {
          PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG_HZ
        } else {
          PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG
        }

        query(ctx, info.preScreenSql,
          Some(Map("carrier" -> info.name, "version" -> param.version,
            param.srcTable -> _srcTable,
            param.targetTable -> PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_PRE_SCREEN)))
    }

    param.carrierInfos.sortBy(_.name).withFilter(info => StringUtils.isNotBlank(info.mpSql))
      .withFilter(info => param.carriers.contains(info.name)).foreach {
      info =>

        val _srcTable = if (info.genType == 1) {
          PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG_HZ
        } else {
          PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG
        }

        query(ctx, info.mpSql,
          Some(Map("carrier" -> info.name, "version" -> param.version,
            param.srcTable -> _srcTable,
            param.targetTable -> PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_MP)))
        persist2Mysql(ctx, info.id, info.name)
    }

  }

  def persist2Mysql(ctx: JobContext2[DPIParam], id: Int, name: String): Unit = {
    val now = DateUtils.getNowTT()
    val _business = ctx.param.business.getOrElse(Seq(-1)).head
    val _userId = ctx.jobCommon.asInstanceOf[DPIJobCommon].userId
    val df = ctx.sql(
      s"""
         |SELECT $id as carrier_id
         |     , version as shard
         |     , tag
         |     , cate_l1
         |     , pattern
         |     , '' as tag_desc
         |     , '$now' as create_time
         |     , '$now' as update_time
         |     , 3 as status
         |     , '' as tag_config
         |     , '${_userId}' as user_id
         |     , '${_business}' as group_id
         |     , tag_group_id
         |FROM ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_MP}
         |WHERE carrier = '$name' and version = '${ctx.param.version}'
         |""".stripMargin)

    ctx.param.jdbcTools.writeToTable(df, "dpi_carrier_tag", SaveMode.Append)

    try {

      ctx.sql(
        s"""
           |SELECT $id as carrier_id
           |     , version as shard
           |     , tag
           |     , cate_l1
           |     , pattern
           |     , '' as tag_desc
           |     , '$now' as create_time
           |     , '$now' as update_time
           |     , 3 as status
           |     , '' as tag_config
           |     , '${_userId}' as user_id
           |     , '${_business}' as group_id
           |     , tag_group_id
           |     ,version
           |FROM ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_MP}
           |WHERE carrier = '$name' and version = '${ctx.param.version}'
           |""".stripMargin).createOrReplaceTempView("tmptable")

      ctx.sql(
        s"""
           |SELECT tag, describe_1, describe_2, version from (
           |SELECT tag, describe_1, describe_2, version
           |FROM ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG}
           |WHERE version = '${ctx.param.version}'
           |union all
           |SELECT tag, describe_1, describe_2, version
           |FROM ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG_HZ}
           |WHERE version = '${ctx.param.version}'
           |) ttt
           |group by tag, describe_1, describe_2, version
           |""".stripMargin
      ).createOrReplaceTempView("t1")

      ctx.spark.table("t1").show(false)

      val df2 = ctx.sql(
        s"""
           |SELECT $id as carrier_id
           |     , a.version as shard
           |     , a.tag
           |     , a.cate_l1
           |     , a.pattern
           |     ,b.describe_1 describe_1
           |     ,b.describe_2 describe_2
           |     , '' as tag_desc
           |     , '$now' as create_time
           |     , '$now' as update_time
           |     , 3 as status
           |     , '' as tag_config
           |     , '${_userId}' as user_id
           |     , '${_business}' as group_id
           |     , a.tag_group_id
           |from
           |tmptable a left join t1 b on a.tag = b.tag and a.version = b.version
           |""".stripMargin
      )
      df2.show(false)
      ctx.param.jdbcTools.writeToTable(df2, "dpi_carrier_tag_new", SaveMode.Append)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
      case e2: Error =>
        e2.printStackTrace()
    }

  }
}
