package com.mob.dataengine.utils.tags

/**
 * @author juntao zhang
 */
class TimestampHandler(updateTime: String) extends Serializable {
  def processTs(currentTsOpt: Option[String], lastTsOpt: Option[String]): Option[String] = {
    // 如果有传入时间则处理进行比较处理,没有则使用时间tag
    try {
      lastTsOpt match {
        case None => Some(updateTime)
        case Some(oldT) => currentTsOpt.map(currentT =>
          if (currentT > oldT) {
            updateTime
          } else {
            currentT
          })
      }
    } catch {
      case ex: Exception =>
        println(s"currentTsOpt=>$currentTsOpt,lastTsOpt=>$lastTsOpt")
        throw ex;
    }
  }

  def accumulateTs(currentTsOpt: Option[String], maxAccumulatorOpt: Option[MaxAccumulator]): Unit = {
    for {
      i <- currentTsOpt
      acc <- maxAccumulatorOpt
    } yield {
      if (acc.value < i.toLong) {
        acc.reset()
        acc.add(i.toString.toLong)
      }
    }
  }

  def processTsWithAcc(currentTsOpt: Option[String], lastTsOpt: Option[String],
    maxAccumulator: Option[MaxAccumulator]): Option[String] = {
    try {
      accumulateTs(currentTsOpt, maxAccumulator)
      processTs(currentTsOpt, lastTsOpt)
    } catch {
      case ex: Exception =>
        println(s"currentTsOpt=>$currentTsOpt,lastTsOpt=>$lastTsOpt")
        throw ex;
    }
  }
}
