package com.mob.dataengine.commons.traits

import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.time.StopWatch

/**
 * @author juntao zhang
 */
trait Watching extends Logging {

  def watch[T](name: String, run: () => T): T = {
    val stopWatch = new StopWatch
    stopWatch.start()
    logger.info(
      s"""
         |
         |----------------------- submit [action=$name] begin... -----------------------
       """.stripMargin)
    val r = run()
    stopWatch.stop()
    val totalSeconds = stopWatch.getTime(TimeUnit.SECONDS)
    logger.info(
      s"""
         |-------------- submit [action=$name] end, cost ${toTimeStr(totalSeconds)} -------------
         |
       """.stripMargin)
    r
  }

  private def toTimeStr(totalSeconds: Long): String = {
    val hours = TimeUnit.SECONDS.toHours(totalSeconds)
    val minutes = TimeUnit.SECONDS.toMinutes(totalSeconds - hours * 3600)
    val seconds = totalSeconds - hours * 3600 - minutes * 60
    val timeStr = if (hours > 0) {
      s"${hours}h${minutes}m${seconds}s"
    } else if (minutes > 0) {
      s"${minutes}m${seconds}s"
    } else {
      s"${seconds}s"
    }
    timeStr
  }
}
