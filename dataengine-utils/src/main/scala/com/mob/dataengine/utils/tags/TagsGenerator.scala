package com.mob.dataengine.utils.tags

import java.util.Properties

import com.mob.dataengine.utils.{DateUtils, PropUtils}
import org.apache.commons.lang3.{RandomStringUtils, StringUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mob.dataengine.commons.utils.{AppUtils, PropUtils => TablePropUtils}
import com.mob.dataengine.utils.tags.profile._
import TagsGeneratorHelper._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ShutdownHookUtils
import org.json4s.DefaultFormats
import scopt.OptionParser


object TagsGenerator {
  //  val taglistLikeFields = Set("taglist", "catelist", "tag_list")

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

  case class Param(
                    day: String = "",
                    profileIds: String = "",
                    hbaseProfileIds: String = "",
                    sample: Boolean = false,
                    batch: Int = 10) {

    import org.json4s.jackson.Serialization.write

    implicit val _ = DefaultFormats

    override def toString: String = write(this)
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Param()
    val projectName = s"TagsGenerator[${DateUtils.currentDay()}]"
    val parser = new OptionParser[Param](projectName) {
      head(s"$projectName")
      opt[String]('p', "profile_ids")
        .text(s"标签id")
        .required()
        .action((x, c) => c.copy(profileIds = x))
      opt[String]('p', "hbase_profile_ids")
        .text(s"需要导入hbase的标签id")
        .action((x, c) => c.copy(hbaseProfileIds = x))
      opt[String]('d', "day")
        .text(s"tag更新时间")
        .action((x, c) => c.copy(day = x))
      opt[Boolean]('s', "sample")
        .text(s"测试")
        .action((x, c) => c.copy(sample = x))
      opt[Int]('b', "batch")
        .text(s"多少个分区做一次checkpoint")
        .action((x, c) => c.copy(batch = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        println(params)
        run(params)
      case _ => sys.exit(1)
    }
  }

  def run(param: Param): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val checkpointPath =
      s"${AppUtils.DATAENGINE_HDFS_TMP}/${param.day}"
    println("checkpoint_path:" + checkpointPath)
    spark.sparkContext.setCheckpointDir(checkpointPath)

    ShutdownHookUtils.addShutdownHook(() => {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(checkpointPath)
      if (fs.exists(outPutPath)) {
        fs.delete(outPutPath, true)
      }
    })

    import spark.implicits._

    val profileIdFulls = param.profileIds.split(",").map(_.trim) // 带有版本号
    val day = param.day
    val sample = param.sample
    val profileIds = profileIdFulls.map(_.split("_").head.toInt)

    Seq(individualProfile, profileMetadata, profileCategory, profileConfidence, midengineStatus).foreach(tb =>
      spark.read.jdbc(url, tb, properties).createOrReplaceTempView(tb)
    )

    val tbManager = TablePartitionsManager(spark)
    val profileIdsTable = "tmp_profileids"
    spark.createDataset(profileIds).toDF("profile_id").createOrReplaceTempView(profileIdsTable)

    val profileIdFullTable = "tmp_profileids_full"
    spark.createDataset(profileIdFulls).toDF("profile_id").createOrReplaceTempView(profileIdFullTable)

    val profileId2CategoryIdMap: Map[Int, Int] = TagsGeneratorHelper.getCategoryIdByProfileIds(spark, profileIdsTable)
    /** profile_id对应的二级分类的id的mapping */
    val profileId2ParentIdMapping: Map[Int, (Int, Int)] = TagsGeneratorHelper.getCategoryId2ParentIdMap(spark)

    /** 计算device的置信度 */
    ConfidenceHelper.buildProfileConfidence(spark, sample)

    val updateTimeMap = TagsGeneratorHelper.profileIdUpdateTime(spark)

    val profiles = TagsGeneratorHelper.getProfiles(spark, profileIdFullTable)
    val fullId2ParentCategoryIdMap = TagsGeneratorHelper.getProfileId2ParentCategoryIdMap(spark)
    val hiveTables = scala.collection.mutable.ArrayBuffer[HiveTable]()

    val dfArr = profiles.groupBy(_.fullTableName).values.zipWithIndex.flatMap { case (ps, j) =>
      val hiveTable = TagsGeneratorHelper.buildHiveTableFromProfiles(spark, sample, ps, tbManager)

      val queries: Seq[String] = if (ps.head.isTimewindowTable) {
        ps.groupBy(_.flagTimewindow).values.zipWithIndex.map { case (tables, i) =>
          scala.util.Try {
            val twTable = TagsGeneratorHelper.buildHiveTableFromProfiles(spark, sample, tables, tbManager)
            hiveTables += twTable
            val featureMapping = s"feature_mapping_${j}_$i"
            val m = TagsGeneratorHelper.getValue2IdMapping(tables.filter(_.hasFeature))
            val mWithPid = m.toSeq.map { case (v, id) => (v, s"${fullId2ParentCategoryIdMap(id)}$pSep$id") }
            val table = tables.head
            val arr = tables.head.profileColumn.split(";").map(_.trim)

            if (table.isFeature) {
              // v3的表应该只有1个值
              s"""
                 |select ${twTable.key} as device,
                 |  concat('${mWithPid.head._2}', '$kvSep', cast(${arr(0)} as string)) as kv,
                 |  ${twTable.updateTimeClause(updateTimeMap.getOrElse(ps.head.fullVersionId, ""), day)} as update_time
                 |from ${twTable.fullTableName}
                 |${twTable.whereClause}
              """.stripMargin
            } else if (table.isTimewindowFlagFeature) {
              spark.sparkContext.broadcast(mWithPid)
              spark.createDataset(mWithPid)
                .toDF("feature_value", "feature_id")
                .createOrReplaceTempView(featureMapping)

              /** 使用inner join来获取对应的id */
              s"""
                 |select ${twTable.key} as device,
                 |  concat(b.feature_id, '$kvSep', cast(${arr(0)} as string)) as kv,
                 |  ${twTable.updateTimeClause(updateTimeMap.getOrElse(ps.head.fullVersionId, ""), day)} as update_time
                 |from ${twTable.fullTableName} as a
                 |inner join $featureMapping as b
                 |on a.feature = b.feature_value
                 |${twTable.whereClause}
              """.stripMargin
            } else {
              s"""
                 |select ${twTable.key} as device,
                 |  concat('${table.fullVersionId}', '$kvSep', cast(${arr(0)} as string)) as kv,
                 |  ${twTable.updateTimeClause(updateTimeMap.getOrElse(ps.head.fullVersionId, ""), day)} as update_time
                 |from  ${twTable.fullTableName}
                 |${twTable.whereClause}
              """.stripMargin
            }
          }
        }.filter(_.isSuccess).map(_.get).toSeq
      } else {
        /** 对类似taglist这样的字段进行集中处理 */
        val ps1 = ps.filter(_.hasTagListField)

        /** 对taglist;7002_001先截取前面的,再分组 */
        val ps1Columns = ps1.groupBy(_.profileColumn.split(";")(0)).map { case (field, arr) =>
          TagsGeneratorHelper.processTaglistLikeFields(spark, arr, field, fullId2ParentCategoryIdMap)
        }.mkString(",")

        val ps2 = ps.filter(!_.hasTagListField)
        val ps2Columns = TagsGeneratorHelper.buildMapStringFromFieldsParent(ps2, kvSep, fullId2ParentCategoryIdMap)

        val columns = if (ps1.isEmpty) {
          s"concat_ws('$pairSep', $ps2Columns)"
        } else if (ps2Columns.isEmpty) {
          s"concat_ws('$pairSep', $ps1Columns)"
        } else {
          s"concat_ws('$pairSep', $ps2Columns, $ps1Columns)"
        }

        val fn = if (hiveTable.isPartitionTable) {
          ""
        } else {
          val tmp = s"processtime_${RandomStringUtils.randomAlphanumeric(5)}"
          hiveTable.registerUpdateTimeUDF(spark, tmp)
          tmp
        }
        Seq(
          s"""
             |select ${hiveTable.key} as device, $columns as kv,
             |  ${hiveTable.updateTimeClause(updateTimeMap.getOrElse(ps.head.fullVersionId, ""), day, fn)} as
             |  update_time
             |from ${hiveTable.fullTableName}
             |${hiveTable.whereClause}
         """.stripMargin
        )
      }

      queries.map(query => sql(spark, query))
    }

    val df = dfArr.sliding(param.batch, param.batch).map(iter => iter.reduce(_ union _).checkpoint()).reduce(_ union _)
    val unionTable = "unionTable"
    df.createOrReplaceTempView(unionTable)

    // broadcast 需要导入hbase的profileid
    val hbasePidSet: Set[String] = if (param.hbaseProfileIds.trim.isEmpty) {
      profileIdFulls.toSet
    } else {
      param.hbaseProfileIds.split(",").map(_.trim).toSet
    }
    val hbasePidSetBC = spark.sparkContext.broadcast(hbasePidSet)
    // 根据父级id聚合数据
    // pid \u0003 profile_id _ version_id
    spark.udf.register("group_by_pid", TagsGeneratorHelper.groupByPid(hbasePidSetBC) _)
    // 去掉前面的父级id
    spark.udf.register("remove_pid", TagsGeneratorHelper.removePid _)
    // 将字符串打成map并去掉map中value为空的kv
    spark.udf.register("cus_str_to_map", TagsGeneratorHelper.customStr2Map _)
    // 去掉没有标签值的标签的置信度
    spark.udf.register("clean_confidence", TagsGeneratorHelper.cleanConfidence _)
    val confidenceIds = TagsGeneratorHelper.getConfidenceProfileIds(spark, profileConfidence)
      .map(t => s"${t._1}_${t._2}")
    val confidenceId2ValueMap =
      s"""
         |map(${confidenceIds.map(fullId => s"'$fullId', tags['$fullId']").mkString(",")})
       """.stripMargin

    sql(spark,
      s"""
         |insert overwrite table ${TablePropUtils.HIVE_TABLE_DM_TAGS_INFO} partition(day='$day')
         |select device, tags, confidence, update_time, grouped_tags
         |from (
         |  select
         |    device, tags, clean_confidence(confidence, $confidenceId2ValueMap) confidence, update_time, grouped_tags
         |  from (
         |    select
         |      device,
         |      remove_pid(tags) tags,
         |      str_to_map(confidence, '$pairSep', '$kvSep') confidence,
         |      update_time update_time,
         |      group_by_pid(tags) grouped_tags
         |    from (
         |      select device,
         |        cus_str_to_map(concat_ws('$pairSep', collect_list(kv)), '$pairSep', '$kvSep') as tags,
         |        max(confidence) as confidence,
         |        max(update_time) as update_time
         |      from (
         |        select device, kv, null as confidence, update_time
         |        from $unionTable
         |        union all
         |        select device, null as kv, confidence, update_time
         |        from  device_confidence_tmp
         |      ) union_confidence
         |      group by device
         |    )a
         |  ) as tmp
         |) as res
         |where tags is not null or confidence is not null
      """.stripMargin)

  }

  def persistUpdateTime(spark: SparkSession, hiveTablesMap: Map[String, HiveTable],
                        profiles: Array[ProfileInfo]): Unit = {
    import spark.implicits._

    val records = profiles.map { p =>
      MidengineStatus(p.profileId, p.profileVersionId, hiveTablesMap(p.fullTableName).updateTime())
    }

    spark.createDataset(records).toDF("profile_id", "profile_version_id", "hive_partition")
      .createOrReplaceTempView("tmp")

    spark.table("tmp").write.mode(SaveMode.Overwrite).jdbc(url, midengineStatus, properties)
  }
}
