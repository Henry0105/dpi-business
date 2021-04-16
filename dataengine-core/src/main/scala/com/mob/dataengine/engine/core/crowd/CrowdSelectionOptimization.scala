package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.engine.core.crowd.selection.optimization.{DevicePre, DeviceSelection}
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, CrowdSelectionOptimizationParam, JobContext}
import com.mob.dataengine.rpc.RpcClient
import org.apache.log4j.Logger

/**
 * 人群包优选: 根据[设备最近活跃时间/指定apppkg最近活跃时间/指定app类别
 * ("http://c.mob.com/pages/viewpage.action?pageId=5648054")最近活跃时间]将输入人群包筛选出指定数量
 *
 * @see (<a href="">详情</a>)
 */
@author("yunlong sun")
@createTime("2018-08-11")
@sourceTable("rp_dataengine.device_opt_cache,rp_dataengine.mobeye_o2o_log_clean")
object CrowdSelectionOptimization extends BaseJob[CrowdSelectionOptimizationParam] {
  @transient private[this] val logger = Logger.getLogger(CrowdSelectionOptimization.getClass)

  override def run(): Unit = {

    val optionTypeSet = jobContext.params.flatMap(p => p.inputs.map(_.option("option_type"))).toSet

    /* 参数校验 */
    if (! optionTypeSet.subsetOf(Set("none", "apppkg", "cate_l1_id", "cate_l2_id"))) {
      logger.error(s"optionType mismatch [$optionTypeSet], eg: [none|apppkg|cate_l1_id|cate_l2_id]")
      sys.exit(1)
    }

    /* 解析输入设备, [从历史设备记录表中直接读取/从dfs系统下载] */
    val sourceData = DevicePre(jobContext).cal("DevicePre")
    if (sourceData.isEmpty) {
      logger.error("DevicePre finished with empty data")
      sys.exit(1)
    }
    logger.info(s"DevicePre finished with [totalCnt=${sourceData.totalCnt}]")

    /* 根据指定条件对设备进行筛选 */
    DeviceSelection(jobContext, sourceData).submit("DeviceSelection")
  }

}

