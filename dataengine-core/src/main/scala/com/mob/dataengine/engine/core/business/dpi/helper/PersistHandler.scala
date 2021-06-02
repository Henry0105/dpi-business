package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.commons.enums.BusinessEnum
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.business.dpi.been.DPIParam
import com.mob.dataengine.engine.core.jobsparam.JobContext2

case class PersistHandler() extends Handler {

  override def handle(ctx: JobContext2[DPIParam]): Unit = {
    persist2Hive(ctx)
  }

  def persist2Hive(ctx: JobContext2[DPIParam]): Unit = {

    ctx.param.business.foreach(_.map { business =>
      ctx.sql(
        s"""
           |INSERT OVERWRITE TABLE ${targetTable(business)} PARTITION (version = '${ctx.param.version}')
           |SELECT tag, url, url_regexp, protocal_type, root_domain, host, file, path, query0, url_key,
           |       source_type, os, cate_l1, period, url_action, describe_1, describe_2, id, date
           |       , '${BusinessEnum.getChineseName(business)}' plat
           |FROM ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG}
           |WHERE version = '${ctx.param.version}'
           |union all
           |SELECT tag, url, url_regexp, protocal_type, root_domain, host, file, path, query0, url_key,
           |       source_type, os, cate_l1, period, url_action, describe_1, describe_2, id, date
           |       , '${BusinessEnum.getChineseName(business)}' plat
           |FROM ${PropUtils.HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG_HZ}
           |WHERE version = '${ctx.param.version}'
           |""".stripMargin)
    })
  }

  def targetTable(businessID: Int): String = {
    businessID match {
      case 1 => ""
      case 2 => PropUtils.HIVE_TABLE_RP_DPI_MARKETPLUS_TAG_URL_MAPPING
      case 3 => PropUtils.HIVE_TABLE_RP_DPI_MOBEYE_TAG_URL_MAPPING
      case 4 => PropUtils.HIVE_TABLE_RP_DPI_GA_TAG_URL_MAPPING
      case 5 => PropUtils.HIVE_TABLE_RP_DPI_FIN_TAG_URL_MAPPING
      case 6 => PropUtils.HIVE_TABLE_RP_DPI_DI_TAG_URL_MAPPING
      case 7 => PropUtils.HIVE_TABLE_RP_DPI_SJHZ_TAG_URL_MAPPING
//      case 8 => PropUtils.HIVE_TABLE_RP_DPI_ZY_TAG_URL_MAPPING
    }
  }
}
