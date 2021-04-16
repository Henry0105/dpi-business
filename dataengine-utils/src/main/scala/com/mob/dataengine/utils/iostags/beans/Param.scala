package com.mob.dataengine.utils.iostags.beans

case class Param(day: String = "") {

  override def toString: String =
    s"""
       |day=$day
       |""".stripMargin
}