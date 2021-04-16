package com.mob.dataengine.engine.core.crowd.helper

import com.mob.dataengine.commons.annotation.code.{sourceTable, targetTable}
import com.mob.dataengine.commons.helper.BitDate
import com.mob.dataengine.engine.core.crowd.CrowdAppTimeFilter.lastPar
import com.mob.dataengine.engine.core.jobsparam.{CrowdAppTimeFilterInputRule, CrowdAppTimeFilterParam, JobContext2}

/**
 * 用户通过appListIndex来指定第几列为需要过滤的appList列表(传入为string,通过appListSep做切分)
 * 和[[CrowdAppTimeFilterAppListHelper]]主要区别为用户可以为每一个device单独定制appList
 */
class CrowdAppTimeFilterInputHelper(ctx: JobContext2[CrowdAppTimeFilterParam]
                                    , rule: CrowdAppTimeFilterInputRule
                                    , tableId: Int) extends CrowdAppTimeFilterTrans(ctx) {

  val tableT2S1 = s"""t2_s1_$tableId"""
  val tableT2PassDeviceTemp = s"""t2_pass_device_temp_$tableId"""
  val tableT2PassDevice = s"""t2_pass_device_$tableId"""
  val tableT2 = s"""t2_$tableId"""
  val tableT3 = s"""t3_$tableId"""
  val tableT4S1 = s"""t4_s1_$tableId"""

  /** 根据timeInterval和appList进行过滤 */
  @sourceTable("t1, input")
  @targetTable("t2")
  override def filterByTimeIntervalAndPkg(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    ctx.sql(
      s"""
         |SELECT device, pkg, app_name
         |     , installed, active, uninstall, new_install
         |     , upper_time
         |     , '1' as tag
         |FROM t1
         |where upper_time >= '${rule.timeIntervalRule._1}'
         |""".stripMargin).createOrReplaceTempView(tableT2S1)

    val hint = if (ctx.matchInfo.idCnt <= 1000000) "/*+ BROADCASTJOIN(input) */" else ""

    val df = ctx.sql(
      s"""
         |SELECT $hint
         |       a.device, pkg, app_name
         |     , installed, active, uninstall, new_install
         |     , upper_time, data, rowId, cardinality(b.apps) as app_nums
         |     , a.tag
         |FROM $tableT2S1 a
         |JOIN (
         |      SELECT device, data, rowId
         |           , transform(split(split(data, '${ctx.param.sep.get}')[${ctx.param.appListIndex}],
         |                   '${ctx.param.appListSep.get}'), x -> lower(x)) as apps
         |      FROM input
         |     ) b
         |ON a.device = b.device and array_contains(b.apps, lower(${rule.appTypeRule}))
         |""".stripMargin)

    df.createOrReplaceTempView(tableT2)
  }

  /** 根据app状态和时间维度(是否包含)筛选app */
  @sourceTable("t2")
  @targetTable("t3")
  override def filterByHitBitDateArr(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    val finalQuery = getFilterByHitBitDateArrQuery()
    ctx.sql(
      s"""
         |CACHE TABLE $tableT3
         |$finalQuery
       """.stripMargin)
  }

  /** 根据pkg数量进行过滤 */
  @sourceTable("t3")
  @targetTable("t4")
  override def filterByPkgNum(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit = {
    val havingCol = filterByPkgNumCondition()
    val rulePosition = ctx.param.inputs.head.rules.get.indexOf(rule) + 1
    ctx.sql(
      s"""
         |SELECT a.device, data, a.rowId
         |FROM $tableT3 a
         |INNER JOIN
         |     (
         |      SELECT device, rowId
         |      FROM (
         |            SELECT device, rowId, tag, first(app_nums) as app_nums
         |            FROM $tableT3
         |            GROUP BY device, pkg, app_name, rowId, tag
         |           ) b
         |      GROUP BY device, rowId
         |      HAVING $havingCol
         |     ) c
         |on a.device = c.device and a.rowId = c.rowId
       """.stripMargin).createOrReplaceTempView(tableT4S1)

    ctx.sql(
      s"""
         |SELECT device, data, rowId
         |FROM $tableT4S1
         |GROUP BY device, data, rowId
       """.stripMargin)
      .createTempView("t4_r_" + tableId)
  }

  def getFilterByHitBitDateArrQuery(): String = {
    val (includeStartTime, includeEndTime) = rule.timeIntervalRule
    val includeStatus = rule.includeRule
    val (includeHitArr, includeMonths) = BitDate.genHitArrInfo(includeStartTime, includeEndTime, lastPar)
    ctx.spark.udf.register("hit_bitDateArr",
      BitDate.hitBitDateArr(includeHitArr, includeMonths, include = includeStatus) _)
    val finalQuery = if (rule.includeRule) {
      s"""
         |SELECT device, pkg, app_name, data, rowId, app_nums, '' as tag
         |FROM $tableT2
         |where hit_bitDateArr(${rule.appStatusRule})
       """.stripMargin
    } else {
      s"""
         |SELECT device, pkg, app_name, data, rowId, app_nums,
         |  case when tag='1' and !hit_bitDateArr(${rule.appStatusRule}) then '1'
         |    else '0' end as tag
         |FROM $tableT2
       """.stripMargin
    }
    finalQuery
  }

  def filterByPkgNumCondition(): String = {
    if (rule.includeRule) {
      val includeHavingCol = rule.numberRestrictRule match {
        case (1, -1) => " count(1) >= 1 "
        case (-1, -1) => s" count(1) = first(app_nums) "
        case (l, r) => s" count(1) >= $l AND count(1) < $r "
      }
      includeHavingCol
    } else {
      val excludeHavingCol = rule.numberRestrictRule match {
        case (1, -1) => " sum(if(tag = '1', 1, 0)) <= 1 "
        case (-1, -1) => s" sum(if(tag = '1', 1, 0)) = 0 "
        case (l, r) => s" sum(if(tag = '1', 1, 0)) >= $l AND sum(if(tag = '1', 1, 0)) < $r "
      }
      excludeHavingCol
    }
  }

}
