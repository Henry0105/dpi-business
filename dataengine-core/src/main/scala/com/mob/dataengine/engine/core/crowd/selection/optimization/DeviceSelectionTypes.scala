package com.mob.dataengine.engine.core.crowd.selection.optimization

import com.mob.dataengine.commons.annotation.code.{author, createTime}

/**
 * 人群包优选类型, 包括:
 * 1. 根据设备最近活跃时间
 * 2. 根据指定apppkg最近活跃时间
 * 3. 根据一级分类最近活跃时间
 * 4. 根据二级分类最近活跃时间
 */
@author("yunlong sun")
@createTime("2018-08-11")
object DeviceSelectionTypes extends Enumeration with Serializable {

  val NONE: DeviceSelectionTypes.Value = Value(1, "none")
  val APPPKG: DeviceSelectionTypes.Value = Value(2, "apppkg")
  val CATEL1: DeviceSelectionTypes.Value = Value(3, "cate_l1_id")
  val CATEL2: DeviceSelectionTypes.Value = Value(4, "cate_l2_id")

}
