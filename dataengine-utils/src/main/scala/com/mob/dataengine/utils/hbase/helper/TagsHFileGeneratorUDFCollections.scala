package com.mob.dataengine.utils.hbase.helper

import com.mob.dataengine.commons.enums.OSName
import com.mob.dataengine.commons.enums.OSName.OSName
import com.mob.dataengine.commons.traits.Logging
import org.apache.spark.broadcast.Broadcast

import scala.collection.BitSet

/**
 * @author xlmeng
 */
trait TagsHFileGeneratorUDFCollections extends TagsHFileGeneratorSide with Logging {

  var profileIdToLevel2CategoryIdBC: Broadcast[Map[String, String]] = _
  var profileIdToLevel2CategoryMockIdBC: Broadcast[Map[String, String]] = _
  var taglistCategoryIdsBroadcast: Broadcast[Map[String, Map[String, Seq[String]]]] = _
  var catelistCategoryIdsBroadcast: Broadcast[Map[String, Map[String, Seq[String]]]] = _
  var tagTfidfCategoryIdsBroadcast: Broadcast[Map[String, Map[String, Seq[String]]]] = _
  val taglistBitSetBC: Broadcast[BitSet] = spark.sparkContext.broadcast(BitSet(taglistPartialArray: _ *))
  val catelisBitSetBC: Broadcast[BitSet] = spark.sparkContext.broadcast(BitSet(catelistPartialArray: _ *))
  val tagTfidfBitSetBC: Broadcast[BitSet] = spark.sparkContext.broadcast(BitSet(tagTfidfPartialArray: _ *))

  val tagListCategoryIdMock = "tagList_mock"
  val cateListCategoryIdMock = "cateList_mock"
  val tagTdifdCategoryIdMock = "tagTdifd_mock"

  private def getFilledMap(input: Map[String, Map[String, Seq[String]]], mockId: String, bitSetBC: Broadcast[BitSet],
                           nullMap: Broadcast[Map[String, Map[String, Seq[String]]]], day: String):
  Map[String, Map[String, Seq[String]]] = {
    lazy val bitSet = bitSetBC.value
    if (input.contains(mockId)) {
      val mockIdMap = input(mockId)
      val likeMap = mockIdMap.filter(e => bitSet.contains(e._1.split("_").head.toInt))
      if (getLastDay(likeMap) == day) {
        val profileIdToLevel2CategoryId = profileIdToLevel2CategoryIdBC.value
        val removeMockMap = mockIdMap.foldLeft(Map.empty[String, Map[String, Seq[String]]])((buffer, input) => {
          val categoryId = profileIdToLevel2CategoryId(input._1)
          buffer.updated(categoryId, buffer.getOrElse(categoryId, Map.empty[String, Seq[String]]) + input)
        })
        input.filterKeys(_ != mockId) ++ nullMap.value ++ removeMockMap
      } else {
        input
      }

    } else {
      input
    }
  }

  /**
   * 对categoryId2value 如果包含有taglist和catelist的categoryId，
   * 补充其他taglist和catelist的categoryId下的taglist和catelist为null。
   */
  def fillTaglist(os: OSName)(m: Map[String, Map[String, Seq[String]]], day: String):
  Map[String, Map[String, Seq[String]]] = {
    if (m == null || m.isEmpty) {
      null
    } else {
      (os: @unchecked) match {
        case OSName.IOS =>
          getFilledMap(m, tagTdifdCategoryIdMock, tagTfidfBitSetBC, tagTfidfCategoryIdsBroadcast, day)
        case OSName.ANDROID =>
          val m2 = getFilledMap(m, tagListCategoryIdMock, taglistBitSetBC, taglistCategoryIdsBroadcast, day)
          getFilledMap(m2, cateListCategoryIdMock, catelisBitSetBC, catelistCategoryIdsBroadcast, day)
      }
    }
  }

  def getLastDay(ms: Map[String, Seq[String]]): String = ms.maxBy(_._2(2))._2(2)

  def getRemainProfiles(ms: Map[String, Seq[String]], day: String): Map[String, Seq[String]] = {
    if (getLastDay(ms) == day) ms else null
  }

  /**
   * 过滤仅留下 profiles在di处的更改时间(数组下标为1) == day的数据
   * ios：在di处的更改时间 == 任务执行时间
   * android：大部分 在di处的更改时间 == 任务执行时间
   * 月更新和重刷模型数据 在di处的更改时间 <= 任务执行时间
   *
   * @param profiles map(profile_id, array(值, 在di处的更改时间, 任务执行时间))
   * @param day      任务日期
   */
  def filterByDay(day: String)(profiles: Map[String, Seq[String]]): Map[String, Seq[String]] = {
    if (null == profiles || profiles.isEmpty) {
      null
    } else {
      getRemainProfiles(profiles, day)
    }
  }

  def filterByDayCateId(day: String)(profiles: Map[String, Map[String, Seq[String]]]):
  Map[String, Map[String, Seq[String]]] = {
    if (null == profiles || profiles.isEmpty) {
      null
    } else {
      profiles.mapValues { ms => getRemainProfiles(ms, day) }.filter(_._2 != null)
    }
  }

  /**
   * 根据profile_id的2级别分类做成
   * |-- device: string (nullable = true)
   * |-- features: map (nullable = true)
   * |    |-- key: map <string, array<string>>
   * |    |-- value: string (valueContainsNull = true)
   * `key` 作为hbase的列名
   * `value` 是一个string类型
   * 这里的分类Id对taglist和catelist做了单独的处理，taglist公用一个categoryId，catelist公用一个categoryId
   *
   * @param profiles map(profile_id, array(值, 在di处的更改时间, 任务执行时间))
   */
  def divideByCateid(isNotMock: Boolean)(profiles: Map[String, Seq[String]]): Map[String, Map[String, Seq[String]]] = {
    if (null == profiles || profiles.isEmpty) {
      null
    } else {
      val profileIdToLevel2CategoryId = if (isNotMock) {
        profileIdToLevel2CategoryIdBC.value
      } else {
        profileIdToLevel2CategoryMockIdBC.value
      }

      profiles.foldLeft(Map.empty[String, Map[String, Seq[String]]])((buffer, input) => {
        val categoryId = profileIdToLevel2CategoryId(input._1)
        buffer.updated(categoryId, buffer.getOrElse(categoryId, Map.empty[String, Seq[String]]) + input)
      })

    }
  }

  def mapValueTrans(_ms: Map[String, Map[String, Seq[String]]]): Map[String, String] = {
    if (null == _ms || _ms.isEmpty) {
      null
    } else {
      _ms.mapValues(ms => mapValueTrans(ms)).filterNot(tups => null == tups || tups._2 == null)
    }
  }

  /**
   * Map(profile_id -> array(profile_value,profile_time,update_time))
   * 1.检查 array(profile_id,profile_time,update_time) 是否为空
   * 2.转换 id_1[[kvSep]]id_2...
   * [[pairSep]]
   * value_1[[pSep]]profile_time_1[[kvSep]]value_2[[pSep]]profile_time_2[[kvSep]]...
   */
  def mapValueTrans(_ms: Map[String, Seq[String]]): String = {
    if (null == _ms || _ms.isEmpty) {
      null
    } else {
      val ms = _ms.filterNot { case (_, v) => null == v || v.size < 2 }
      if (ms.isEmpty) null else {
        val finalValue = ms.foldLeft(("", "")) { case (buffer, input) =>
          (s"${buffer._1}$kvSep${input._1}", s"${buffer._2}$kvSep${input._2.take(2).mkString(pSep)}")
        }
        // 去除开头多余的kvSep
        s"${finalValue._1.replaceFirst(kvSep, "")}$pairSep${finalValue._2.replaceFirst(kvSep, "")}"
      }
    }
  }

}
