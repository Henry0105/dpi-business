package com.mob.dataengine.engine.core.business.dpi

import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.utils.BusinessScriptUtils
import com.mob.dataengine.engine.core.business.dpi.been.{DPIParam, CarrierInfo}
import com.mob.dataengine.engine.core.business.dpi.helper._
import com.mob.dataengine.engine.core.jobsparam.{BaseJob2, JobContext2}
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.sql.{Column, DataFrame}

object DpiMktUrl extends BaseJob2[DPIParam] {

  override def run(ctx: JobContext2[DPIParam]): Unit = {
    prepare(ctx)
    // 过程链调用
    val chain = new HandlerChain()
    chain.addHandler(InputHandler())
    chain.addHandler(CleanDataAndParseURLHandler())
    chain.addHandler(AssumeUniqueHandler())
    chain.addHandler(GenTagHandler())
    chain.addHandler(DeclineHandler())
    chain.addHandler(PersistHandler())
    chain.execute(ctx)

    sendRPC(ctx)
  }

  def prepare(ctx: JobContext2[DPIParam]): Unit = {
    // 持有锁
    ctx.param.jdbcTools.executeUpdateWithoutCheck(
      s"UPDATE dpi_job_lock SET locked=2,update_time='${DateUtils.getNowTT()}' " +
        s"WHERE version='${ctx.param.version}'")

    val df = ctx.param.jdbcTools.readFromTable(ctx.spark, "dpi_carrier")

    ctx.param.carrierInfos = df.collect().map { r =>
      val id = r.getAs[Int]("id")
      val name = r.getAs[String]("name_en").trim
      val genTagSql = r.getAs[String]("gen_tag")
      val preScreenSql = r.getAs[String]("pre_screen")
      val mpSql = r.getAs[String]("mp")
      val genType = r.getAs[Int]("gen_type")

      CarrierInfo(id, name, genTagSql, preScreenSql, mpSql, genType)
    }

  }

  def query(ctx: JobContext2[DPIParam], queries: Seq[String], replaces: Option[Map[String, String]]) {
    queries.foreach { rawQuery =>
      query(ctx, rawQuery, replaces)
    }
  }

  def query(ctx: JobContext2[DPIParam], rawQuery: String, replaces: Option[Map[String, String]]) {
    val query = if (replaces.isDefined) {
      replaces.get.foldLeft(rawQuery) { (context, kv) => context.replaceAll(s"\\$$${kv._1}", kv._2) }
    } else {
      rawQuery
    }
    val queries = BusinessScriptUtils.getQueries(query)
    queries.foreach(ctx.sql)
  }

  override def cacheInput: Boolean = true

  override def sendRPC(ctx: JobContext2[DPIParam]): Unit = {
    println(s"rpc返回:\u0001${ctx.param.output.uuid}\u0002${ctx.param.cbBean.toJson}")
    if (ctx.jobCommon.hasRPC()) {
      RpcClient.send(ctx.jobCommon.rpcHost, ctx.jobCommon.rpcPort,
        s"2\u0001${ctx.param.output.uuid}\u0002${ctx.param.cbBean.toJson}")
    }
  }

  override def transformData(df: DataFrame, ctx: JobContext2[DPIParam]): DataFrame = ctx.spark.emptyDataFrame

  override def createMatchIdsCol(ctx: JobContext2[DPIParam]): Seq[Column] = Seq.empty
}
