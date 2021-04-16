package com.mob.dataengine.commons

import java.io.BufferedInputStream
import java.net.{URI, URL}

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * @author juntao zhang
 */
object SrcURLReader {
  def getTgz(srcURL: String): List[String] = {
    val d = new ListBuffer[String]()
    val ins = new TarArchiveInputStream(new GzipCompressorInputStream(
      new BufferedInputStream(new URL(srcURL).openStream())
    ))
    var f = true
    while (f) {
      val e = ins.getNextTarEntry
      if (e == null) {
        f = false
      } else {
        if (!e.isDirectory) {
          val arr = e.getName.split("/")
          // 忽略以`.`开头的文件
          if (!arr(arr.length - 1).startsWith(".")) {
            d ++= Source.fromInputStream(ins).getLines().map(_.toString.trim).filter(_.nonEmpty).toList
          }
        }
      }
    }
    ins.close()
    d.toList
  }

  def getFile(srcURL: String): List[String] = {
    Source.fromURL(
      srcURL
    ).getLines().map(_.toString.trim).filter(_.nonEmpty).toList
  }

  def retryWithTimes[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e if n > 1 =>
        println(s"retry $n")
        e.printStackTrace()// TODO use log4j
        Thread.sleep(3 * 1000)
        retryWithTimes(n - 1)(fn)
    }
  }

  def main(args: Array[String]): Unit = {
    // val srcURL = "http://10.5.1.45:20101/fs
    // /download?module=ad_marketing&path=ad_marketing/354/2018-06-12/新建文本文档 (2).txt"
    val srcURL = "http://10.21.131.11:20101" +
      "/fs/download?path=demo/sample3.tar.gz&module=demo"
    getTgz(srcURL)
    //      val url = new java.net.URL(srcURL)
    //      Source.fromURL(
    //        srcURL.replace(url.getQuery, java.net.URLEncoder.encode(url.getQuery, "UTF-8"))
    //      ).getLines().map(_.toString.trim).filter(_.nonEmpty).toList
  }

  def get(srcURL: String, compressType: String): List[String] = {
    if (compressType == "tgz") {
      getTgz(srcURL)
    } else {
      getFile(srcURL)
    }
  }

  // TODO 千万数据量得采用hdfs
  def toRDD(spark: SparkSession, srcURL: String, compressType: String, numSlices: Int = 4): RDD[String] = {
    val srcData = retryWithTimes(3) { get(new URI(srcURL).toASCIIString(), compressType) }
    spark.sparkContext.parallelize(
      srcData, math.max(4, srcData.length / 1000000)
    ).distinct()
  }
}
