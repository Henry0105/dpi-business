package com.mob.dataengine.engine.core.lookalike

import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.engine.core.lookalike.pca.{JobContext, PCATagsCalType}
import org.apache.log4j.Logger

@author("yunlong sun")
@createTime("2018-07-23")
object PCATagsTfidfMapping {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // TODO send err msg
    if (1 != args.length) {
      println(s"Usage: /opt/mobdata/sbin/spark-submit... <jsonParam>")
      System.exit(-1)
    }

    /* 计算模块总数, 含预处理(输入数据解析) */
    val jobNums = 12
    /* 上下文对象 */
    val jc = JobContext(None, args(0), useLocal = false)

    PCATagsCalType(jc.job.param.calType.value) match {
      case PCATagsCalType.FULL =>
      // PCATagsTfidfMappingFull(jc).submit()
      case PCATagsCalType.INCR =>
      // PCATagsTfidfMappingIncr(jc).submit()
    }
  }
}
