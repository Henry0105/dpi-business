package com.mob.dataengine.utils.iostags

import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.utils.{AppUtils, PropUtils}
import com.mob.dataengine.utils.iostags.beans.{IosProfileInfo, IosTagsGeneratorIncrParam, QueryUnitContext}
import com.mob.dataengine.utils.iostags.handle.QueryUnitFactory
import com.mob.dataengine.utils.iostags.helper.TagsGeneratorHelper.{kvSep, pairSep, sql}
import com.mob.dataengine.utils.iostags.helper.{MetaDataHelper, TablePartitionsManager, TagsGeneratorHelper}
import org.apache.hadoop.fs.FileSystem._
import org.apache.hadoop.fs.Path
import org.apache.spark.ShutdownHookUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

/**
 * @author xlmeng
 */
object IosTagsGeneratorIncr {
  val tagsTable = "tags_table"
  val confidenceTable = "confidence_table"

  def main(args: Array[String]): Unit = {
    val defaultParams = IosTagsGeneratorIncrParam()
    val projectName = s"TagsGenerator[${DateUtils.getCurrentDay()}]"
    val parser = new OptionParser[IosTagsGeneratorIncrParam](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .required()
        .text(s"tag更新时间")
        .action((x, c) => c.copy(day = x))
      opt[Boolean]('s', "sample")
        .optional()
        .text(s"数据是否采样")
        .action((x, c) => c.copy(sample = x))
      opt[Int]('b', "batch")
        .optional()
        .text(s"多少个分区做一次checkpoint")
        .action((x, c) => c.copy(batch = x))
      opt[Boolean]('f', "full")
        .optional()
        .text(s"多少个分区做一次checkpoint")
        .action((x, c) => c.copy(full = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(p) =>
        println(p)
        val spark: SparkSession = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()
        if (p.full) {
          new IosTagsGeneratorFullFirst(spark, p).run()
        } else {
          new IosTagsGeneratorIncr(spark, p).run()
        }
        spark.close()
      case _ =>
        println(s"参数有误:${args.mkString(",")}")
        sys.exit(1)
    }
  }

}

class IosTagsGeneratorIncr(@transient spark: SparkSession, p: IosTagsGeneratorIncrParam) extends Serializable {

  import IosTagsGeneratorIncr._

  var confidenceId2ValueMap: String = _

  def run(): Unit = {
    // 去mysql读取标签的元数据
    prepare()
    // 查询到tags和confidence信息
    handle()
    // 持久化到hive
    persist2Hive()
  }

  def prepare(): Unit = {
    setCheckPoint()
    TagsGeneratorHelper.registerNum2Str(spark)
  }

  /**
   * 工厂类去创建sql组成单元，组成执行的sql，执行sql得出结果
   */
  def query(profileInfos: Array[IosProfileInfo], cxt: QueryUnitContext, isConfidence: Boolean = false): DataFrame = {
    val queryUnits = QueryUnitFactory.createQueryUnit(cxt, profileInfos, isConfidence)
    val dfArr = queryUnits.map(queryUnit => sql(spark, queryUnit.query()))

    dfArr.sliding(p.batch, p.batch).map(iter => iter.reduce(_ union _).cache().checkpoint())
      .reduce(_ union _)
  }

  def handle(): Unit = {
    val tbManager: TablePartitionsManager = TablePartitionsManager(spark)
    val cxt = QueryUnitContext(spark, p.day, tbManager, p.sample, p.full)

    handleTags(cxt: QueryUnitContext)
    handleConfidence(cxt: QueryUnitContext)
  }

  def handleTags(cxt: QueryUnitContext): Unit = {
    val profileInfos = MetaDataHelper.getComputedProfiles(spark, p.day, p.full)
    val tagsDF = query(profileInfos, cxt)
    tagsDF.createOrReplaceTempView(tagsTable)
  }

  def handleConfidence(cxt: QueryUnitContext): Unit = {
    val confidenceInfos = MetaDataHelper.getProfileConfidence(spark, p.day, p.full)
    val confidenceDF = query(confidenceInfos, cxt, isConfidence = true)
    confidenceDF.createOrReplaceTempView(confidenceTable)
  }

  def persist2Hive(): Unit = {
    spark.udf.register("kv_str_to_map", new TagsGeneratorHelper.kvStr2map)

    sql(spark,
      s"""
         |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_DI_SEC} PARTITION (day='${p.day}')
         |SELECT ifid, tags, confidence
         |FROM   (
         |        SELECT ifid
         |             , kv_str_to_map(kv, '$pairSep', '$kvSep', update_time) as tags
         |             , kv_str_to_map(confidence, '$pairSep', '$kvSep', update_time) as confidence
         |        FROM (
         |              SELECT ifid, kv, null as confidence, update_time
         |              FROM   $tagsTable
         |              UNION ALL
         |              SELECT ifid, null AS kv, confidence, update_time
         |              FROM   $confidenceTable
         |             ) res_1
         |        GROUP BY ifid
         |       ) res_2
         |WHERE  tags is not null
         |""".stripMargin)

    println("IOS标签增量表写入成功")
  }

  def setCheckPoint() {
    val checkpointPath =
      s"${AppUtils.DATAENGINE_HDFS_TMP}/ios_tags_v2/${p.day}"
    println("checkpoint_path:" + checkpointPath)
    spark.sparkContext.setCheckpointDir(checkpointPath)

    ShutdownHookUtils.addShutdownHook(() => {
      val fs = get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(checkpointPath)
      if (fs.exists(outPutPath)) {
        fs.delete(outPutPath, true)
      }
    })
  }

}
