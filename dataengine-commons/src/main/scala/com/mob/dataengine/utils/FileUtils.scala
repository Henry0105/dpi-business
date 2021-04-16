package com.mob.dataengine.utils

import java.io.File

import scala.io.Source

object FileUtils {

  def getSqlScript(_path: String, tableName: String): String = {
    val path = _path.replace("/", File.separator)
    val rootPath = System.getProperty("user.dir")

    val rootPathParent = rootPath.substring(0, rootPath.lastIndexOf(File.separator) + 1)

    val fullPath = rootPathParent + path

    val fileContent = Source.fromFile(fullPath, "UTF-8")
    val tableFull = fileContent.mkString
    val tableFound = tableFull.split(";").filter(table => table.contains(tableName)).head.replaceAll("`", "")

    fileContent.close()
    tableFound
  }

  def getJson(_path: String): String = {
    val path = _path.replace("/", File.separator)
    val rootPath = System.getProperty("user.dir")

    val rootPathParent = rootPath.substring(0, rootPath.lastIndexOf(File.separator) + 1)

    val fullPath = rootPathParent + path

    val fileContent = Source.fromFile(fullPath, "UTF-8")

    val jsonContent = fileContent.mkString.stripMargin

    fileContent.close()
    jsonContent
  }

}
