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
object AreaTypeEnum extends Enumeration with Serializable {

  type AreaTypeEnum = Value

  // 绘制区域 输入的是一堆经纬度
  val LAT_LON_LIST: AreaTypeEnum.Value = Value(1)
  // 选择区域 输入区域编号
  val AREA_CODE: AreaTypeEnum.Value = Value(2)


}
