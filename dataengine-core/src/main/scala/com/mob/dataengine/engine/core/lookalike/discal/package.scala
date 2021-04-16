package com.mob.dataengine.engine.core.lookalike

import com.mob.dataengine.commons.pojo.{ParamInput, ParamOutput}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

package object discal {


  case class Param(input: ParamInput, output: ParamOutput)

  case class Job(
    jobId: String,
    jobName: String,
    param: Array[Param],
    rpcHost: String,
    rpcPort: Int
  ) {
    override def toString: String = s"(jobId=$jobId,jobName=$jobName,param=${param.mkString(",")}" +
      s",rpcHost=$rpcHost,rpcPort=$rpcPort)"
  }

  object Job {
    implicit val _ = DefaultFormats

    def apply(json: String): Job = {

      JsonMethods.parse(json).transformField {
        case ("job_id", x) => ("jobId", x)
        case ("job_name", x) => ("jobName", x)
        case ("rpc_host", x) => ("rpcHost", x)
        case ("rpc_port", x) => ("rpcPort", x)
        case ("input", x) => ("input", x.transformField {
          case ("compress_type", y) => ("compressType", y)
          case ("input_type", x2) => ("inputType", x2)
          case ("id_type", x4) => ("idType", x4 \ "value")
        })
        case ("output", x1) => ("output", x1.transformField({
          case ("uuid", x2) => ("uuid", x2)
          case ("module", x2) => ("module", x2)
          case ("value", x2) => ("value", x2)
        }))
      }
    }.extract[Job]
  }
}
