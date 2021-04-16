package com.mob.dataengine.commons

import com.mob.dataengine.commons.enums.DeviceType.DeviceType
import com.mob.dataengine.commons.enums.InputType.InputType

/**
 * @param inputType 输入数据包类型
 * @param idType 种子数据的类型
 * @param sep 行数据的分隔符
 * @param headers 数据对应的列名(可选)
 * @param idx 主数据对应的列号
 * @param uuid 数据标识
 * @param encrypt 加密类型
 */
case class SeedSchema(inputType: InputType, idType: Int, sep: String,
  var headers: Seq[String], idx: Int, var uuid: String, encrypt: Encrypt) {

  // 这个只会在输入为sql的时候使用到
  def idField: String = headers(idx - 1)

  //  获取对应的key时, 使用的query
  val query: String = s"$idType,${encrypt.encryptType}"
}
