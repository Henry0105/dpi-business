package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.engine.core.business.dpi.been.DPIParam
import com.mob.dataengine.engine.core.jobsparam.JobContext2

case class AssumeUniqueHandler() extends Handler {

  /** 检查: 输入的URL不能有重复 如果有重复，需要返回回调信息 */
  override def handle(ctx: JobContext2[DPIParam]): Unit = {
    val rpNum = ctx.sql(
      s"""
         |SELECT max(cnt)
         |FROM (
         |      SELECT url_regexp, url_key, count(1) as cnt
         |      FROM dpi_tb3
         |      GROUP BY url_regexp, url_key
         |     ) a
         |""".stripMargin).collect().map(_.getAs[Long](0)).head

    if (rpNum > 1) {
      ctx.param.cbBean.setError(1)
      setSuccessor(None)
      ctx.param.jdbcTools.executeUpdateWithoutCheck(
        s"UPDATE dpi_job_lock SET locked=0,update_time='${DateUtils.getNowTT()}' WHERE version='${ctx.param.version}'")

    }
  }
}
