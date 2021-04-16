package com.mob.dataengine.engine.core.crowd.selection.optimization

import com.mob.dataengine.commons.DeviceSrcReader
import com.mob.dataengine.commons.annotation.code._
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.{CrowdSelectionOptimizationParam, JobContext}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
 * 输入数据准备, [从历史设备记录表中直接加载/从dfs系统加载计算]
 *
 * @param jobContext JobContext任务上下文
 */
@author("yunlong sun")
@createTime("2018-08-11")
case class DevicePre(jobContext: JobContext[CrowdSelectionOptimizationParam]) {
  @transient private[this] lazy val logger: Logger = Logger.getLogger(this.getClass)
  @transient private[this] lazy val spark = jobContext.spark
  private[this] var totalCnt: Long = 0L

  import spark._
  import spark.implicits._

  /* 清洗设备结果表-旧 */
  private def isInOldTable(uuid: String) = {
    sql(s"show partitions ${PropUtils.HIVE_TABLE_MOBEYE_O2O_LOG_CLEAN}")
      .collect().map(_.getAs[String](0).split("\\/")(1)).contains(s"userid=$uuid")
  }

  /* 执行计算 */
  def cal(moduleName: String): SourceData = {
    var dfArr: ArrayBuffer[DataFrame] = ArrayBuffer[DataFrame]()
    /* 存在历史记录,直接加载 */
    jobContext.params.foreach { p =>
      println(p)
      val _sourceDF = {
          // 比如：11.5 G  34.5 G  /user/hive/warehouse/rp_dataengine.db/mobeye_o2o_log_clean
          // /day=20180810/userid=556_5b6d06887ddf3fed37c9488f
          // 4,9986,4714
          p.inputs.map(input =>
            if (isInOldTable(input.uuid)) {
              table(PropUtils.HIVE_TABLE_MOBEYE_O2O_LOG_CLEAN)
                .filter($"userid".equalTo(input.uuid)).select("device")
            } else {
              val idRDD = DeviceSrcReader.toRDD2(jobContext.spark, input).distinct()
              idRDD.cache()
              idRDD.toDF("device")
            }
          )
      }
      dfArr += _sourceDF.reduce(_.union(_))
    }

    totalCnt = dfArr.reduce(_.union(_)).count()
    SourceData(dfArr.reduce(_.union(_)), totalCnt)
  }
}
