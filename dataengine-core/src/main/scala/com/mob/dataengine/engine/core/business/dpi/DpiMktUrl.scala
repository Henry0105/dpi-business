package com.mob.dataengine.engine.core.business.dpi

import com.mob.dataengine.commons.{DPIJobCommon, JobCommon}
import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.utils.BusinessScriptUtils
import com.mob.dataengine.engine.core.business.dpi.been.{CarrierInfo, DPIParam, TagInfo}
import com.mob.dataengine.engine.core.business.dpi.helper._
import com.mob.dataengine.engine.core.jobsparam.{BaseJob2, JobContext2, JobParamTransForm}
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.sql.{Column, DataFrame}

object DpiMktUrl extends BaseJob2[DPIParam] {

  override def getJobCommon(arg: String): JobCommon = {
    JobParamTransForm.humpConversion(arg)
      .extract[DPIJobCommon]
  }

  override def run(ctx: JobContext2[DPIParam]): Unit = {
    prepare(ctx)
    // 过程链调用
    val chain = new HandlerChain()
    chain.addHandler(InputHandler())
    chain.addHandler(CleanDataAndParseURLHandler())
    chain.addHandler(AssumeUniqueHandler())
    chain.addHandler(GenTagV3Handler())
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

    val business = ctx.param.business.getOrElse(Seq.empty[Int]).mkString("('", "','", "')")
    ctx.param.tagInfoDF = ctx.param.jdbcTools.readFromTable(ctx.spark,
      s"""
         |(
         |SELECT s1.carrier_id, s1.shard, s1.tag, s1.pattern, s1.status, s1.user_id, s1.group_id, s2.name, s2.gen_type
         |FROM dpi_carrier_tag s1
         |JOIN dpi_carrier s2 on s1.carrier_id = s2.id
         |JOIN dpi_group_user s3 on s1.group_id = s3.group_id
         |WHERE s1.group_id IN ${business}
         |AND s1.status = 1
         |AND DATE_FORMAT(s1.over_time,'%Y%m%d') > DATE_FORMAT(NOW(),'%Y%m%d')
         |) T
         |""".stripMargin
    )
    ctx.param.tagInfos = ctx.param.tagInfoDF.collect()
      .map(r => {
        TagInfo(r.getAs[Int]("carrier_id"),
          r.getAs[String]("shard"), r.getAs[String]("tag"), r.getAs[String]("pattern"),
          r.getAs[Int]("status"), r.getAs[String]("user_id"), r.getAs[Int]("group_id"),
          r.getAs[String]("name"), r.getAs[Int]("gen_type"))
      })

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

  def query2DF(ctx: JobContext2[DPIParam], rawQuery: String, replaces: Option[Map[String, String]]): Seq[DataFrame] = {
    val query = if (replaces.isDefined) {
      replaces.get.foldLeft(rawQuery) { (context, kv) => context.replaceAll(s"\\$$${kv._1}", kv._2) }
    } else {
      rawQuery
    }
    val queries = BusinessScriptUtils.getQueries(query)
    queries.map(ctx.sql)
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
