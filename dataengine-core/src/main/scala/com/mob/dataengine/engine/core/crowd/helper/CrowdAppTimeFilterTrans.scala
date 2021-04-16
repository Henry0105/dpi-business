package com.mob.dataengine.engine.core.crowd.helper

import com.mob.dataengine.commons.annotation.code.{sourceTable, targetTable}
import com.mob.dataengine.engine.core.jobsparam.{CrowdAppTimeFilterInputRule, CrowdAppTimeFilterParam, JobContext2}
import org.apache.spark.sql.DataFrame

object CrowdAppTimeFilterTrans {

  def apply(ctx: JobContext2[CrowdAppTimeFilterParam]
            , rule: CrowdAppTimeFilterInputRule
            , tableId: Int): CrowdAppTimeFilterTrans = {
    if (rule.appListRule(ctx.param.inputTypeEnum, ctx.param.inputs.head.appListSep).isDefined) {
      new CrowdAppTimeFilterAppListHelper(ctx, rule, tableId)
    } else {
      new CrowdAppTimeFilterInputHelper(ctx, rule, tableId)
    }
  }
}

abstract class CrowdAppTimeFilterTrans(ctx: JobContext2[CrowdAppTimeFilterParam]) {

  // 1.时间筛选：一年内的时间任意筛选  [[filterByHitBitDateArr]]
  // 2.app状态:在装、活跃、卸载、新装  [[filterByHitBitDateArr]]
  // 3.是否包含目标app：包含/不包含 [[filterByHitBitDateArr]]
  // 4.app名称/app包名的筛选 [[filterByTimeIntervalAndPkg]]
  // 5.device的app数量的筛选 [[filterByPkgNum]]
  def execute(df: DataFrame): Unit = {
    filterByTimeIntervalAndPkg(ctx)
    filterByHitBitDateArr(ctx)
    filterByPkgNum(ctx)
  }



  /** 根据timeInterval和appList进行过滤 */
  @sourceTable("t1, input")
  @targetTable("t2")
  def filterByTimeIntervalAndPkg(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit

  /** 根据app状态和时间维度(是否包含)筛选app */
  @sourceTable("t2")
  @targetTable("t3")
  def filterByHitBitDateArr(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit

  /** 根据pkg数量进行过滤 */
  @sourceTable("t3")
  @targetTable("t4")
  def filterByPkgNum(ctx: JobContext2[CrowdAppTimeFilterParam]): Unit

}