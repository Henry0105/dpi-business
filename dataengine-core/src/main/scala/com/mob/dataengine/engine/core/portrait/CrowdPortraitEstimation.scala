package com.mob.dataengine.engine.core.portrait

import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, CrowdPortraitEstimationParam, JobContext}
import com.mob.dataengine.engine.core.portrait.crowd.estimation._
import org.apache.log4j.Logger

// TODO MDML-25-RESOLVED 需要加上说明
/**
 * 自定义群体标签(画像)估算, 含:
 *
 * @see 家居画像: CP001(<a href="http://c.mob.com/pages/viewpage.action?pageId=5656135">详情</a>)
 * @see 服饰箱包画像: CP002(<a href="http://c.mob.com/pages/viewpage.action?pageId=5649126">详情</a>)
 */
@author("yunlong sun")
@createTime("2018-06-25")
object CrowdPortraitEstimation extends BaseJob[CrowdPortraitEstimationParam] {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  def prepare(jobContext: JobContext[CrowdPortraitEstimationParam]): Unit = {
    /* 注册临时函数 */
    jobContext.spark.sql("drop temporary function if exists explode_tags")
    jobContext.spark.sql("create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags'")
    /* 动态分区 */
    jobContext.spark.sql("set hive.exec.dynamic.partition=true")
    jobContext.spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }


  override def run(): Unit = {
    /* 计算模块总数, 含预处理(输入数据解析) */
    /* 上下文对象 */
    /* 检查输入参数 */
    prepare(jobContext)
    val labels = jobContext.params.flatMap(_.inputs.flatMap(_.tagList))
    if (labels.toSet.isEmpty || (labels.nonEmpty && !labels.contains("CP001") && ! labels.contains("CP002"))) {
      logger.error(s"Labels empty/mismatch error[$labels], eg: [CP001|CP002]")
      sys.exit(1)
    }
    /* 解析后的输入数据及布隆数组 */
    val sourceDataOpt: Option[SourceData] = DeviceIdTagsMapping(jobContext, None).cal("DeviceIdTagsMapping")
    /* 检查匹配结果 */
    if (sourceDataOpt.isEmpty) {
      logger.info(s"PreDevice finished with empty data")
      sys.exit(1)
    }
    /* 计算家居画像 */
    EstimationFurnitureScore(jobContext, sourceDataOpt).submit("EstimationFurnitureScore")
    EstimationClothingScore(jobContext, sourceDataOpt).submit("EstimationClothingScore")
  }
}
