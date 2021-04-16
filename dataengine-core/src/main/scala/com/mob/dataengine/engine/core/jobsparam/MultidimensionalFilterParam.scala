package com.mob.dataengine.engine.core.jobsparam

import java.util.Properties

import com.mob.dataengine.commons.utils.AppUtils
import com.mob.dataengine.commons.{BaseParam2, Encrypt, JobInput2, JobOutput2}

/**
 * 多维过滤输入参数
 * @param uuid    输入数据的uuid分区 或者 sql语句
 * @param include 标签过滤(包含)
 * @param exclude 标签过滤(不包含)
 * @param idx     使用sql的时候需要指定 device的列数 以1开始
 * @param sep     使用sql的时候需要制定拼接分隔符 默认 "\u0001"
 */
case class MultidimensionalFilterInput(override val uuid: String,
                                  override val idType: Int = 4,
                                  override val sep: Option[String] = None,
                                  override val header: Int = 0,
                                  override val idx: Option[Int] = None,
                                  override val headers: Option[Seq[String]] = None,
                                  override val encrypt: Encrypt = Encrypt(0),
                                  override val inputType: String,
                                  val numContrast: Option[Map[String, String]] = None,
                                  val include: Option[Map[String, String]] = None,
                                  val exclude: Option[Map[String, String]] = None)
extends JobInput2(uuid, idType, sep, header, idx, headers, encrypt, inputType) with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, include=$include, exclude=$exclude,numContrast=$numContrast
     """.stripMargin
}

/**
 * 多维过滤输出参数
 * @param uuid        输出的uuid分区
 * @param hdfsOutput  输出hdfs地址
 * @param limit       输出行数限制
 */
case class MultidimensionalFilterOutput(override val uuid: String,
                                   override val hdfsOutput: String = "",
                                   override val limit: Option[Int] = Some(-1),
                                   override val keepSeed: Int = 1)
  extends JobOutput2(uuid, hdfsOutput, limit, keepSeed)


class MultidimensionalFilterParam(override val inputs: Seq[MultidimensionalFilterInput],
  override val output: MultidimensionalFilterOutput)
  extends BaseParam2(inputs, output) with Serializable {
  var matchCnt = 0L
  val (url, properties): (String, Properties) = initMysql()
  val deviceString: String = "id"
  val colList: List[String] = List(deviceString, "tags")
  var inputIDCount = 0L

  def initMysql(): (String, Properties) = {
    val properties = new Properties()
    val ip: String = AppUtils.TAG_MYSQL_JDBC_IP
    val port: Int = AppUtils.TAG_MYSQL_JDBC_PORT
    val user: String = AppUtils.TAG_MYSQL_JDBC_USER
    val pwd: String = AppUtils.TAG_MYSQL_JDBC_PWD
    val db: String = AppUtils.TAG_MYSQL_JDBC_DB
    val url = s"jdbc:mysql://$ip:$port/$db?useUnicode=true&amp;characterEncoding=UTF-8?autoReconnect=true"

    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("driver", "com.mysql.jdbc.Driver")

    (url, properties)
  }


}
