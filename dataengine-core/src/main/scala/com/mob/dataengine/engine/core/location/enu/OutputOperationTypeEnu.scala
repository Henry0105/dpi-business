package com.mob.dataengine.engine.core.location.enu

object OutputOperationTypeEnu extends Enumeration{

  type OutputOperationTypeEnu = Value
  val save2HiveTable: OutputOperationTypeEnu.Value = Value(0)
  val cacheTable: OutputOperationTypeEnu.Value = Value(1)

}
