package com.mob.dataengine.utils.tags.fulltags.enums

object PartitionType extends Enumeration {

  type PartitionType = Value

  val none: PartitionType = Value(0, "none")
  val timewindow: PartitionType = Value(1, "timewindow")
  val flag: PartitionType = Value(2, "flag")
  val feature: PartitionType = Value(4, "feature")

  val timewindowFlag: PartitionType = Value(3, "timewindow_flag")
  val timewindowFeature: PartitionType = Value(5, "timewindow_feature")
  val flagFeature: PartitionType = Value(6, "flag_feature")
  val timewindowFlagFeature: PartitionType = Value(7, "timewindow_flag_feature")

  def isTimewinodw(that: Int): Boolean = that >= timewindowFlag.id

  def hasFeature(that: Int): Boolean = that > timewindowFlag.id

  def isFeature(that: Int): Boolean = that == feature.id

  def isTimewindowFlagFeature(that: Int): Boolean = that == timewindowFlagFeature.id
}
