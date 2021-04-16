package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.engine.core.business.dpi.been.DPIParam
import com.mob.dataengine.engine.core.jobsparam.JobContext2

trait Handler {

  protected var _successor: Option[Handler] = None

  def successor(): Option[Handler] = {
    _successor
  }

  def setSuccessor(handler: Option[Handler]): Unit = {
    _successor = handler
  }

  def execute(ctx: JobContext2[DPIParam]): Unit = {
    println(this.getClass.getSimpleName + " ==> ")
    handle(ctx)
    successor().foreach(_.execute(ctx))
  }

  def handle(ctx: JobContext2[DPIParam]): Unit

}
