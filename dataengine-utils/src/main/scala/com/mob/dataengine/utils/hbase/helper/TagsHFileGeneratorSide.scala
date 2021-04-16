package com.mob.dataengine.utils.hbase.helper

import java.util.Properties

import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.utils.PropUtils

/**
 * @author xlmeng
 */
trait TagsHFileGeneratorSide {

  val kvSep = "\u0001"
  val pairSep = "\u0002"
  val pSep = "\u0003"

  val ip: String = PropUtils.getProperty("tag.mysql.ip")
  val port: Int = PropUtils.getProperty("tag.mysql.port").toInt
  val user: String = PropUtils.getProperty("tag.mysql.user")
  val pwd: String = PropUtils.getProperty("tag.mysql.password")
  val db: String = PropUtils.getProperty("tag.mysql.database")
  val url = s"jdbc:mysql://$ip:$port/$db?useUnicode=true&amp;characterEncoding=UTF-8?autoReconnect=true"

  val properties = new Properties()
  properties.setProperty("user", user)
  properties.setProperty("password", pwd)
  properties.setProperty("driver", "com.mysql.jdbc.Driver")

  lazy val taglistProfileIdArray: Array[String] =
    MetadataUtils.findTaglistLikeProfiles("tag_list;").values.toArray
  lazy val catelistProfileIdArray: Array[String] =
    MetadataUtils.findTaglistLikeProfiles("catelist;").values.toArray
  lazy val tagTfidfProfileIdArray: Array[String] =
    MetadataUtils.findTaglistLikeProfiles("tag_tfidf;").values.toArray

  lazy val taglistPartialArray: Array[Int] = taglistProfileIdArray.map(_.split("_").head.toInt)
  lazy val catelistPartialArray: Array[Int] = catelistProfileIdArray.map(_.split("_").head.toInt)
  lazy val tagTfidfPartialArray: Array[Int] = tagTfidfProfileIdArray.map(_.split("_").head.toInt)

}
