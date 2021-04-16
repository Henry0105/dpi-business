package com.mob.dataengine.commons.utils

import java.io.File

import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.commons.enums.BusinessEnum.BusinessType
import com.mob.dataengine.core.constants.DataengineExceptionType
import com.mob.dataengine.core.utils.DataengineExceptionUtils
import org.apache.log4j.Logger

import scala.collection.Iterator
import scala.io.Source

@author("xlmeng")
@createTime("20201119")
object BusinessScriptUtils {
  private[this] lazy val logger: Logger = Logger.getLogger(PropUtils.getClass)

  private lazy val rootDir: File = {
    val business = BusinessScriptUtils.getClass.getClassLoader.getResource("business")
    new File(business.toURI)
  }

  private lazy val businessDirs: Array[File] = rootDir.listFiles()

  /** 得到业务线目录 */
  def getBusinessDir(businessType: BusinessType): File = {
    val maybeFile = businessDirs.find(_.getName == businessType.toString)
    if (maybeFile.isEmpty) {
      throw DataengineExceptionUtils.apply("目录未创建或者目录下没有文件", DataengineExceptionType.READ_PATH_IS_NULL)
    }
    maybeFile.get
  }

  def getQueriesFromFile(fileName: String): Seq[String] = {
    val filePath = BusinessScriptUtils.getClass.getClassLoader.getResource(fileName)
    val source = Source.fromFile(filePath.toURI)
    getQueries(source.getLines())
  }

  def getQueries(businessDir: File, scriptName: String): Seq[String] = {
    val path: String = s"${businessDir.getPath}/$scriptName"
    val source = Source.fromFile(path, "UTF-8")
    getQueries(source.getLines())
  }

  def getQueries(str: String): Seq[String] = {
    getQueries(str.split("\n").iterator)
  }

  /** 得到具体sql执行语句 */
  def getQueries(_lines: Iterator[String]): Seq[String] = {
    val lines = _lines.filterNot(_.isEmpty).map(_.trim)

    lines.foldLeft(List[String]()) { case (queries, line) =>
      queries match {
        case Nil => List(line)
        case init :+ last =>
          if (last.endsWith(";")) {
            queries :+ line
          } else {
            val queryWithNextLine = last + "\n" + line
            init :+ queryWithNextLine
          }
      }
    }.map { str =>
      if (str.endsWith(";")) str.substring(0, str.length - 1) else str
    }
  }


}

