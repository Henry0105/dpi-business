package com.mob.dataengine.commons.utils

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.mob.dataengine.commons.exceptions.DataengineException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import java.util.Properties

import scala.collection.mutable.ArrayBuffer

/**
 * @author juntao zhang
 */
case class JdbcTools(ip: String, port: Int, db: String, user: String, password: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val url = s"jdbc:mysql://$ip:$port/$db?useUnicode=true&amp;characterEncoding=UTF-8?autoReconnect=true"
  val connectionProperties: Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${user}")
    connectionProperties.put("password", s"${password}")
    connectionProperties
  }

  def find[T](sql: String, apply: ResultSet => T): Seq[T] = {
    val fields = new ArrayBuffer[T]()
    val conn = getConnect()
    val (stat, rs) = executeQuery(sql, conn)
    try {
      while (rs.next()) {
        fields += apply(rs)
      }
    } finally {
      close(stat, rs)
      closeConn(conn)
    }
    fields
  }

  def getConnect(): Connection = {
    classOf[com.mysql.jdbc.Driver]
    DriverManager.getConnection(url, user, password)
  }

  def executeQuery(sql: String, conn: Connection): (Statement, ResultSet) = {
    val statement = conn.createStatement()
    val rs = statement.executeQuery(sql)
    (statement, rs)
  }

  def close(statRS: (Statement, ResultSet)): Unit = {
    val rs = statRS._1
    val stat = statRS._2
    try {
      if (rs != null && !rs.isClosed) {
        rs.close()
      }
      if (stat != null && !stat.isClosed) {
        stat.close()
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def closeConn(connection: Connection): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.close()
    }
  }

  def findOne[T](id: Long, tableName: String, apply: ResultSet => T): Option[T] = {
    if (id <= 0) {
      return None
    }
    findOne(s"select * from $tableName where id = $id", apply)
  }

  def findOne[T](sql: String, apply: ResultSet => T): Option[T] = {
    val conn = getConnect()
    val (stat, rs) = executeQuery(sql, conn)
    try {
      if (rs.next()) {
        Some(apply(rs))
      } else {
        logger.info(s"[$sql] not find.")
        None
      }
    } finally {
      close(stat, rs)
      closeConn(conn)
    }
  }

  def closeConn(conStatRS: (Connection, Statement, ResultSet)): Unit = {
    val rs = conStatRS._1
    val stat = conStatRS._2
    val conn = conStatRS._3
    try {
      if (rs != null && !rs.isClosed) {
        rs.close()
      }
      if (stat != null && !stat.isClosed) {
        stat.close()
      }
      if (conn != null && !conn.isClosed) {
        conn.close()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def executeUpdate(sql: String): Long = {
    logger.info(sql)
    val conn = getConnect()
    val statement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    val result = statement.executeUpdate()
    if (result < 1) {
      throw new DataengineException(s"executeUpdate failed[$sql]")
    }
    val generatedKeys: ResultSet = statement.getGeneratedKeys
    println(s"executeUpdate res: result=$result, generatedKeys=${generatedKeys.next()}")
    val id = if (generatedKeys.next()) {
      generatedKeys.getLong(1)
    } else {
      throw new DataengineException(s"executeUpdate GeneratedKeys failed[$sql]")
    }
    conn.close()
    id
  }

  def executeUpdateWithoutCheck(sql: String): Unit = {
    logger.info(sql)
    val conn = getConnect()
    val statement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    val result = statement.executeUpdate()
    if (result < 1) {
      throw new DataengineException(s"executeUpdate failed[$sql]")
    }
    conn.close()
  }

  def executeUpdate(sql: String, paramsSeq: Seq[Seq[_]]): Seq[Long] = {
    logger.info(
      s"""
         |
         |---------------------------
         |sql:$sql
         |paramsSeq:
         |${paramsSeq.map(_.mkString("\t")).mkString("\n")}
         |---------------------------
         |
       """.stripMargin)
    val conn = getConnect()
    conn.setAutoCommit(false)
    val statement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    paramsSeq.foreach(params => {
      params.indices.foreach(idx => {
        params(idx) match {
          case p: String =>
            statement.setString(idx + 1, p)
          case p: Int =>
            statement.setInt(idx + 1, p)
          case p: Long =>
            statement.setLong(idx + 1, p)
          case p: Boolean =>
            statement.setBoolean(idx + 1, p)
          case _ =>
            throw new DataengineException(s"executeUpdate unsupported type ${params(idx)}=>${params(idx).getClass}")
        }
      })
      statement.addBatch()
    })
    val results = statement.executeBatch()
    if (results.count(r => r > 0) != paramsSeq.size) {
      throw new DataengineException(s"executeUpdate failed[$sql]")
    }
    val ids = new ArrayBuffer[Long]()
    val generatedKeys = statement.getGeneratedKeys
    while (generatedKeys.next()) {
      ids += generatedKeys.getLong(1)
    }
    conn.commit()
    conn.close()
    ids
  }

  /**
   * 事务执行批量sql
   */
  def executeUpdate(connection: Connection, sqlList: Array[String]): Boolean = {
    var isSuccess = true
    try {
      connection.setAutoCommit(false)
      val statement = connection.createStatement()
      statement.execute("insert into t_project() ")
      sqlList.foreach(sql => {
        statement.addBatch(sql)
      })
      val ids = statement.executeBatch()
      connection.commit()
    } catch {
      case e: Exception =>
        connection.rollback()
        isSuccess = false
        e.printStackTrace()
    } finally {
      connection.close()
    }
    isSuccess
  }

  def executeUpdate(sqlList: Array[String]): Boolean = {
    val connection = this.getConnect()
    var isSuccess = true
    try {
      connection.setAutoCommit(false)
      val statement = connection.createStatement()
      sqlList.foreach(sql => {
        statement.addBatch(sql)
      })
      statement.executeBatch()
      connection.commit()
    } catch {
      case e: Exception =>
        connection.rollback()
        isSuccess = false
        e.printStackTrace()
    } finally {
      connection.close()
    }
    isSuccess
  }

  def readFromTable(spark: SparkSession, tableName: String): DataFrame = {
    spark.read.jdbc(url, tableName, connectionProperties)
  }

  def writeToTable(df: DataFrame, tableName: String, mode: SaveMode): Unit = {
    df.write
      .mode(SaveMode.Append) // <--- Append to the existing table
      .jdbc(url, tableName, connectionProperties)
  }

  def writeToTable(df: DataFrame, tableName: String, mode: String): Unit = {
    df.write
      .mode(mode) // <--- Append to the existing table
      .jdbc(url, tableName, connectionProperties)
  }

}
