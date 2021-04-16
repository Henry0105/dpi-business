package com.mob.dataengine.engine.core.business.dpi.helper

import com.mob.dataengine.engine.core.business.dpi.been.DPIParam
import com.mob.dataengine.engine.core.jobsparam.JobContext2

class HandlerChain {

  private var length = 0
  // 虚拟头结点
  private val head = new Handler {
    override def handle(ctx: JobContext2[DPIParam]): Unit = {}
  }

  private var tail = head

  def size(): Int = {
    length
  }

  def addHandler(handler: Handler): Unit = {
    tail.setSuccessor(Some(handler))
    tail = tail.successor().get
    length += 1
  }

  def execute(ctx: JobContext2[DPIParam]): Unit = {
    head.execute(ctx)
  }
}
