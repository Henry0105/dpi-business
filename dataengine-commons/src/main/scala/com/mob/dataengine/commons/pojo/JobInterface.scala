package com.mob.dataengine.commons.pojo

import org.apache.commons.lang3.StringUtils

/**
 * @author juntao zhang
 */
trait JobInterface {
  val jobId: String
  val jobName: String
  val rpcHost: String
  val rpcPort: Int
  val param: Seq[ParamInterface]

  def hasRPC(): Boolean = {
    StringUtils.isNotBlank(rpcHost) && rpcPort != 0
  }
}
