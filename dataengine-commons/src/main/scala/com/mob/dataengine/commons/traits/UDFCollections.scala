package com.mob.dataengine.commons.traits

import com.mob.dataengine.commons.utils.{AesHelper, Md5Helper}
import org.apache.commons.lang3.StringUtils

trait UDFCollections extends Serializable{
  // spark udf
  def md5Array(arr: Seq[String]): String = {
    if (null == arr || arr.isEmpty) {
      null
    } else {
      arr.map(Md5Helper.entryMD5_32).mkString(",")
    }
  }

  def trim2Imei14(arr: Seq[String]): Seq[String] = {
    if (null == arr || arr.isEmpty) {
      null
    } else {
      arr.map(_.substring(0, 14))
    }
  }

  def aesArray(arr: Seq[String], key: String, iv: String): String = {
    if (null == arr || arr.isEmpty) {
      null
    } else {
      arr.map(e => AesHelper.encodeAes(key, iv, e)).mkString(",")
    }
  }

  def aes(str: String, key: String, iv: String): String = {
    if (null == str || str.trim.isEmpty) {
      null
    } else {
      AesHelper.encodeAes(key, iv, str)
    }
  }

  def latest(arr: Seq[String], tm: Seq[String], matchLimit: Int): String = {
    if (arr != null && arr.nonEmpty) {
      (if (matchLimit < 0) {
        tm.zip(arr).sortWith((a, b) => a._1 > b._1)
      } else {
        tm.zip(arr).sortWith((a, b) => a._1 > b._1).slice(0, matchLimit)
      }).map(_._2).mkString(",")
    } else {
      null
    }
  }

  def clean_map(ids: Map[Int, String]): Map[Int, String] = {
    if (null == ids) {
      null
    } else {
      val tmp = ids.filter{ case (id, v) => StringUtils.isNotBlank(v)}
      if (tmp.isEmpty) {
        null
      } else {
        tmp
      }
    }
  }

  /**
   * @param arr id数组
   * @param ltm id数组对应的时间
   * @param matchLimit 最多匹配的条数
   * @return 返回新id和对应的时间
   */
  def latestArray(arr: Seq[String], ltm: Seq[String], matchLimit: Int): Seq[String] = {
    if (arr != null && arr.nonEmpty) {
      val ord = Ordering.Tuple2(Ordering.String.reverse, Ordering.String.reverse)
      (if (matchLimit < 0) {
        ltm.zip(arr).sorted(ord)
      } else {
        ltm.zip(arr).sorted(ord).slice(0, matchLimit)
      }).map(_._2)
    } else {
      null
    }
  }

  /**
   * @param ltm id数组对应的时间
   * @param matchLimit 最多匹配的条数
   * @return 返回新id和对应的时间
   */
  def latestLtmArray(ltm: Seq[String], matchLimit: Int): Seq[String] = {
    if (ltm != null && ltm.nonEmpty) {
      val ord = Ordering.String.reverse
      if (matchLimit < 0) {
        ltm.sorted(ord)
      } else {
        ltm.sorted(ord).slice(0, matchLimit)
      }
    } else {
      null
    }
  }

  def substringArray(arr: Seq[String], from: Int, to: Int): Seq[String] = {
    if (null == arr || arr.isEmpty) {
      null
    } else {
      val tmpArr = arr.flatMap{ e =>
        if (e.length >= to) {
          Some(e.substring(from, to))
        } else {
          None
        }
      }
      if (tmpArr.isEmpty) null else tmpArr
    }
  }

  def substring14Array(arr: Seq[String]): Seq[String] = {
    substringArray(arr, 0, 14)
  }

  def substring15Array(arr: Seq[String]): Seq[String] = {
    substringArray(arr, 0, 15)
  }

  // 去掉输出中带有字母的imei
  def cleanImeiArray(imei: Seq[String]): Seq[String] = {
    if (null == imei || imei.isEmpty) {
      null
    } else {
      val res = imei.filter(s => s matches """^\d+$""")
      if (res.nonEmpty) {
        res
      } else {
        null
      }
    }
  }

  // 去掉带有字母的imei的对应的时间
  def cleanLtmArrayByImei(imei: Seq[String], imeiLtm: Seq[String]): Seq[String] = {
    if (null == imei || imei.isEmpty || null == imeiLtm || imeiLtm.isEmpty) {
      null
    } else {
      val res = imei.zip(imeiLtm).filter(tuple => tuple._1 matches """^\d+$""").map(_._2)
      if (res.nonEmpty) {
        res
      } else {
        null
      }
    }
  }
}
