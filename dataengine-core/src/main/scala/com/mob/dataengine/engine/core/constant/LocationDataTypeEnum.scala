package com.mob.dataengine.engine.core.constant


/**
 * #Owner: zhangnw
 * #describe: 地理围栏 数据选择方式
 * #projectName:mobeye
 * #BusinessName:o2o
 * #SourceTable:
 * #TargetTable:
 * #CreateDate: 2017/9/27 18:31
 */
object LocationDataTypeEnum extends Enumeration with Serializable{

  type LocationDataTypeEnum = Value

  /**
   * 常驻
   */
  val MONTHLY: LocationDataTypeEnum.Value = Value(1)
  /**
   * 流动
   */
  val DAILY: LocationDataTypeEnum.Value = Value(2)


}
