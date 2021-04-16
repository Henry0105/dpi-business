package com.mob.dataengine.engine.core.crowd.helper

import com.mob.dataengine.commons.annotation.code.{sourceTable, targetTable}
import com.mob.dataengine.commons.enums.InputType
import com.mob.dataengine.commons.helper.BitDate
import com.mob.dataengine.engine.core.crowd.CrowdAppTimeFilter.lastPar
import com.mob.dataengine.engine.core.jobsparam.{CrowdAppTimeFilterInputRule, CrowdAppTimeFilterParam, JobContext2}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel


// 提供了appList参数的任务，直接根据appList进行app_name/pkg过滤
class CrowdAppTimeFilterAppListHelper(ctx: JobContext2[CrowdAppTimeFilterParam]
                                      , rule: CrowdAppTimeFilterInputRule
                                      , tableId: Int) extends CrowdAppTimeFilterTrans(ctx) {

  val tableT2S1 = s"""t2_s1_$tableId"""
  val tableT2PassDeviceTemp = s"""t2_pass_device_temp_$tableId"""
  val tableT2PassDevice = s"""t2_pass_device_$tableId"""
  val tableT2 = s"""t2_$tableId"""
  val tableT3 = s"""t3_$tableId"""
  val tableT4S1 = s"""t4_s1_$tableId"""
  val tableT4S2 = s"""t4_s2_$tableId"""

  /** 根据timeInterval和appList进行过滤 */
  @sourceTable("t1, input")
  @targetTable("t2")
  override def filterByTimeIntervalAndPkg(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    val appListRuleData = rule.appListRule(ctx.param.inputTypeEnum, ctx.param.inputs.head.appListSep)
    val appList = appListRuleData.get.map(app => s"'${app.toLowerCase}'").mkString("(", ",", ")")
    println("", appList.mkString)
    ctx.sql(
      s"""
         |SELECT device, pkg, app_name
         |     , installed, active, uninstall, new_install
         |     , upper_time
         |FROM t1
         |WHERE upper_time >= '${rule.timeIntervalRule._1}' AND lower(${rule.appTypeRule}) in $appList
         |""".stripMargin).createOrReplaceTempView(tableT2S1)

    if (!rule.includeRule) {
      ctx.sql(
        s"""
           |SELECT device
           |     , '' as installed, '' as active, '' as uninstall, '' as new_install, '' as upper_time
           |from t1
           |where upper_time >= ${rule.timeIntervalRule._1}
           |group by device
       """.stripMargin).createOrReplaceTempView(tableT2PassDeviceTemp)
      createTwoStageTable(tableT2PassDeviceTemp, tableT2PassDevice)
    }
    createTwoStageTable(tableT2S1, tableT2)

  }


  /** 根据app状态和时间维度(是否包含)筛选app */
  @sourceTable("t2")
  @targetTable("t3")
  override def filterByHitBitDateArr(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    val (includeStartTime, includeEndTime) = rule.timeIntervalRule
    val includeStatus = rule.includeRule
    val (includeHitArr, includeMonths) = BitDate.genHitArrInfo(includeStartTime, includeEndTime, lastPar)
    ctx.spark.udf.register("hit_bitDateArr",
      BitDate.hitBitDateArr(includeHitArr, includeMonths, include = includeStatus) _)

    val condition = if (rule.includeRule) {
      s"""where hit_bitDateArr(${rule.appStatusRule})"""
    } else {
      s"""where !hit_bitDateArr(${rule.appStatusRule})"""
    }
    val filteredDF = ctx.sql(
      s"""
         |SELECT device, pkg, app_name, data, rowId
         |FROM $tableT2
         |$condition
       """.stripMargin
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    filteredDF.createOrReplaceTempView(tableT3)
    println(s"根据app状态和时间维度(是否包含)筛选后数量: ${filteredDF.count()}")
  }

  /** 根据pkg数量进行过滤 */
  @sourceTable("t3")
  @targetTable("t4")
  override def filterByPkgNum(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    val condition = filterByPkgNumCondition()
    ctx.sql(
      s"""
         |SELECT device
         |      FROM (
         |            SELECT device
         |            FROM $tableT3
         |            GROUP BY device, pkg, app_name
         |           ) b
         |      GROUP BY device
         |      HAVING $condition
       """.stripMargin).createOrReplaceTempView(tableT4S1)

    val query = if (rule.includeRule) {
      s"""
         |SELECT a.device, data, rowId
         |FROM $tableT3 a
         |INNER JOIN $tableT4S1 b on a.device = b.device
       """.stripMargin
    } else {
      s"""
         |select a.device, a.data, a.rowId
         |from $tableT2PassDevice a
         |left join $tableT4S1 b
         |on a.device = b.device
         |where b.device is null
         """.stripMargin
    }

    ctx.sql(query).createOrReplaceTempView(tableT4S2)

    ctx.sql(
      s"""
         |SELECT device, data, rowId
         |FROM $tableT4S2
         |GROUP BY device, data, rowId
       """.stripMargin)
      .createOrReplaceTempView("t4_r_" + tableId)
  }

  def createTwoStageTable(sourceTableName: String, resultTableName: String): Unit = {
    val df = if (InputType.nonEmpty(ctx.param.inputTypeEnum)) {
      val hint = if (ctx.matchInfo.idCnt <= 1000000) "/*+ BROADCASTJOIN(input) */" else ""
      ctx.sql(
        s"""
           |SELECT $hint
           |       a.device, pkg, app_name
           |     , installed, active, uninstall, new_install
           |     , upper_time, data, rowId
           |FROM $sourceTableName a
           |JOIN input
           |ON a.device = input.device
           |""".stripMargin)
    } else {
      ctx.spark.table(sourceTableName)
        .withColumn("data", lit(""))
        .withColumn("rowId", col("device"))
    }
    df.createOrReplaceTempView(resultTableName)
  }

  def filterByPkgNumCondition(): String = {
    if (rule.includeRule) {
      val appList = rule.appListRule(ctx.param.inputTypeEnum, ctx.param.inputs.head.appListSep)
      rule.numberRestrictRule match {
        case (1, -1) => " count(1) >= 1 "
        case (-1, -1) => s" count(1) = ${appList.get.length} "
        case (l, r) => s" count(1) >= $l AND count(1) < $r "
      }
    } else {
      rule.numberRestrictRule match {
        case (1, -1) => " count(1) <= 1 "
        case (-1, -1) => s" count(1) >= 1 "
        case (l, r) => s" count(1) >= $l AND count(1) < $r "
      }
    }
  }
}
