package com.mob.dataengine.commons.service

import java.util.UUID

import com.mob.dataengine.commons.SeedSchema
import com.mob.dataengine.commons.enums.InputType
import com.mob.dataengine.commons.traits.{Logging, TableTrait}
import com.mob.dataengine.commons.utils.PropUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataHubService {

  /**
   * 读取种子数据,并根据传入的参数做成一个df
   * 1. 当输入为sql的时候,直接spark去读sql处理
   * 2. 当输入为uuid/dfs的时候,需要根据传入的schema去生成一个df
   */
  def readSeed(seedSchema: SeedSchema): DataFrame

  // 查询出对应的根据种子包的信息,查询出对应的key
  def deduceIndex(seedSchema: SeedSchema): String

  def deduceIndex(id: String, encryptType: String): String

  def writeToHub(df: DataFrame, uuid: String): Unit

  def writeToHDFS(df: DataFrame, hdfsPath: String, options: Map[String, String]): Unit

  def statFeature(df: DataFrame, ids: Seq[String]): Map[String, Long]
}

object DataHubServiceImpl {
  val filterByIdsFn = "filter_by_ids"
  val seedField: String = "seed"
  val hubTable: String = PropUtils.HIVE_TABLE_DATA_HUB
  val codeMappingTable: String = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING
}

case class DataHubServiceImpl(@transient spark: SparkSession)
  extends Logging with DataHubService with TableTrait {

  import DataHubServiceImpl._
  import spark.implicits._

  var isLoadSeed: Boolean = false
  var key: String = _
  val codeMappingDay: String = getLastPar(codeMappingTable).split("=")(1)

  sql(
    s"""
       |select code, en, cn, query
       |from $codeMappingTable
       |where day = '$codeMappingDay'
        """.stripMargin).cache().createOrReplaceTempView("cached_code_mapping")

  /**
   * 1. 将种子数据现在python端都落入seed里面(除了sql)
   * 2. 将种子数据根据参数进行解析并将idx对应的数据放入相应的key下
   * 3. 之后做关联的时候,使用2中的key下面的数据去关联
   */
  override def readSeed(seedSchema: SeedSchema): DataFrame = {
    if (!isLoadSeed) {
      key = loadSeed(seedSchema)
      isLoadSeed = true
    }

    sql(
      s"""
         |select feature, feature['$key'][0] id
         |from $hubTable
         |where uuid = '${seedSchema.uuid}'
        """.stripMargin)
  }

  // 将seed字段提取idx进去相应的key
  // key的枚举参见: http://c.mob.com/pages/viewpage.action?pageId=37784120
  def loadSeed(seedSchema: SeedSchema): String = {
    val key = deduceIndex(seedSchema)
    if (seedSchema.inputType.equals(InputType.SQL)) {
      // 因为传入的sql做了转义,这里需要转回来
      val ret = sql(s"""${seedSchema.uuid.replaceAll("@", "'")}""")
      seedSchema.headers = ret.schema.fieldNames // 这里设置一下schema
      ret.createOrReplaceTempView("sql_table")
      val uuid = UUID.randomUUID().toString
      seedSchema.uuid = uuid
      val seedClause = if (ret.schema.fieldNames.length < 2) {
        s"array(${ret.schema.fieldNames.head})"
      } else {
        s"array('${seedSchema.sep}', ${ret.schema.fieldNames.mkString(",")})"
      }

      logger.info("load sql seed")
      sql(
        s"""
           |insert overwrite table $hubTable partition (uuid='$uuid')
           |select  -- 将种子包和对应的key都放入数据
           |  map(
           |    '$seedField', $seedClause,
           |    '$key', array(${seedSchema.idField})
           |  )
           |from sql_table
          """.stripMargin)
    } else if (seedSchema.inputType.equals(InputType.DFS)) {
      // 将dfs的下的内容进行覆盖插入提取出来的数据
      logger.info("load dfs seed")
      sql(
        s"""
           |insert overwrite table $hubTable partition (uuid='${seedSchema.uuid}')
           |select map_concat(
           |  map(
           |    '$seedField', feature['$seedField'],
           |    '$key', array(split(feature['$seedField'][0], '${seedSchema.sep}')[${seedSchema.idx - 1}])
           |  ), feature
           |)
           |from $hubTable
           |where uuid = '${seedSchema.uuid}'
          """.stripMargin)
    } else {
      // 当inputType为uuid的时候,表示任务数据是从上个任务带过来的, 则不需要load数据
    }
    key
  }

  // 根据种子包的信息,查询对应的feature中的key
  // 目前根据idType和encrypt来查询对应的key
  override def deduceIndex(seedSchema: SeedSchema): String = {
    deduceIndex(seedSchema.query)
  }


  override def deduceIndex(id: String, encryptType: String): String = {
    val query: String = s"$id,$encryptType"
    deduceIndex(query)
  }

  def deduceIndex(query: String): String = {
    sql(
      s"""
         |select code
         |from cached_code_mapping
         |where query = '$query'
        """.stripMargin)
      .map(_.getString(0)).head()
  }

  // 将数据写入hive表
  override def writeToHub(df: DataFrame, uuid: String): Unit = {
    df.createOrReplaceTempView("res_tb")
    sql(
      s"""
         |insert overwrite table $hubTable partition(uuid='$uuid')
         |select feature
         |from res_tb
        """.stripMargin)
  }

  /**
   * 将数据写入hdfs
   *
   * @param options 写入的格式要求, 设置source用来表示存储的类型
   */
  override def writeToHDFS(df: DataFrame, hdfsPath: String, options: Map[String, String]): Unit = {
    df.write.options(options)
      .mode(options.getOrElse("mode", "overwrite"))
      .format(options.getOrElse("source", "csv")).save(hdfsPath)
  }

  /**
   * 将feature的数据进行统计, 返回 id -> 统计值
   */
  def statFeature(df: DataFrame, ids: Seq[String]): Map[String, Long] = {
    import spark.implicits._
    val idsBC = spark.sparkContext.broadcast(ids)

    val cntDF = df.mapPartitions(iter => {
      val counts = iter.flatMap { r =>
        val feature = r.getAs[Map[String, Seq[String]]]("feature")
        feature.filterKeys(idsBC.value.toSet.contains)
          .filter { case (_, seq) => seq.length >= 2 && StringUtils.isNotBlank(seq(1)) }.keys
      }.foldLeft(Map.empty[String, Long]) { (acc, s) =>
        acc ++ Map(s -> (acc.getOrElse(s, 0L) + 1L))
      }

      counts.toIterator
    }).toDF("pid", "cnt").cache()

    cntDF.createOrReplaceTempView("cnt_df")

    sql("SELECT pid, sum(cnt) AS scnt FROM cnt_df GROUP BY pid")
      .map(row =>
        (row.getAs[String]("pid"), row.getAs[Long]("scnt"))
      ).collect().toMap
  }

  def filterByIds(m: Map[String, Seq[String]], ids: Set[String]): Map[String, Seq[String]] = {
    val res = m.filterKeys(ids.contains)
    if (res.isEmpty) {
      null
    } else {
      res
    }
  }

}
