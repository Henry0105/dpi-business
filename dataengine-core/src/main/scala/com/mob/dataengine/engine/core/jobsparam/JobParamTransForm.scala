package com.mob.dataengine.engine.core.jobsparam

import org.json4s.JField
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods

object JobParamTransForm {
  lazy private val transformer: PartialFunction[JField, JField] = {
    case ("job_id", x) => ("jobId", x)
    case ("job_name", x) => ("jobName", x)
    case ("rpc_host", x) => ("rpcHost", x)
    case ("rpc_port", x) => ("rpcPort", x)
    case ("id_type", x) => ("idType", x)
    case ("id_types", x) => ("idTypes", x)
    case ("hdfs_output", x) => ("hdfsOutput", x)
    case ("encrypt_type", x) => ("encryptType", x)
    case ("param_id", x) => ("paramId", x)
    case ("input_type", x) => ("inputType", x)
    case ("data_process", x) => ("dataProcess", x)
  }

  def humpConversion(json: String): JValue = {
    JsonMethods.parse(json)
      .transformField(JobParamTransForm.transformer)
  }
}
