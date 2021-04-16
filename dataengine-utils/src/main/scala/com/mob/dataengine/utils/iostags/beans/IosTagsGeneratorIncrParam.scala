package com.mob.dataengine.utils.iostags.beans

case class IosTagsGeneratorIncrParam(
                                      day: String = "",
                                      sample: Boolean = false,
                                      batch: Int = 10,
                                      full: Boolean = false) {

  override def toString: String =
    s"""
       |day=$day, sample=$sample, batch=$batch, full=$full
       |""".stripMargin
}