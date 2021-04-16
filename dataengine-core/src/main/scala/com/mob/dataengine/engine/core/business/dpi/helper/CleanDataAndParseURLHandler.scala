package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.commons.utils.{BusinessScriptUtils, PropUtils}
import com.mob.dataengine.engine.core.business.dpi.DpiMktUrl.query
import com.mob.dataengine.engine.core.business.dpi.been.DPIParam
import com.mob.dataengine.engine.core.jobsparam.JobContext2

case class CleanDataAndParseURLHandler() extends Handler {

  /**
   * 步骤一: 数据清洗
   * 步骤二: 解析URL
   * 步骤三: 获取root domain
   * table transform: input => dpi_tb1 => dpi_tb2 => dpi_tb3
   */
  override def handle(ctx: JobContext2[DPIParam]): Unit = {
    val queries = BusinessScriptUtils.getQueriesFromFile("cleandate_parseurl.sql")
    query(ctx, queries, None)
  }

}
