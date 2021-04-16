package com.mob.dataengine.utils.tags.fulltags

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.{Locale, Properties}

import com.mob.dataengine.commons.utils.{AppUtils, PropUtils => TablePropUtils}
import com.mob.dataengine.utils.tags.fulltags.profile.FullTagsGeneratorHelper._
import com.mob.dataengine.utils.tags.fulltags.profile._
import com.mob.dataengine.utils.{DateUtils, PropUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ShutdownHookUtils
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import scopt.OptionParser

object FullTagsGenerator {
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
    sample: Boolean = false,
    batch: Int = 10) {

    import org.json4s.jackson.Serialization.write

    implicit val _ = DefaultFormats

    override def toString: String = write(this)(DefaultFormats)
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Param()
    val projectName = s"FullTagsGenerator[${DateUtils.currentDay()}]"
    val parser = new OptionParser[Param](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .text(s"运行时间")
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
      s"${AppUtils.DATAENGINE_HDFS_TMP}/fullTags/${param.day}"
    println("checkpoint_path:" + checkpointPath)
    spark.sparkContext.setCheckpointDir(checkpointPath)

    ShutdownHookUtils.addShutdownHook(() => {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(checkpointPath)
      if (fs.exists(outPutPath)) {
        fs.delete(outPutPath, true)
      }
    })

    //num2str UDF
    val dft = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
    dft.setMaximumFractionDigits(340) // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS
    val dfBC = spark.sparkContext.broadcast(dft)
    spark.udf.register("num2str", (d: Double) => {
      dfBC.value.format(d)
    })

    import spark.implicits._

    val day = param.day
    val sample = param.sample

    //注册mysql表
    Seq(individualProfile, profileMetadata, profileCategory, profileConfidence,
      midengineStatus, profileVersion).foreach(tb =>
      spark.read.jdbc(url, tb, properties).createOrReplaceTempView(tb)
    )


    val tbManager = TablePartitionsManager(spark)

    /** 计算每个device的置信度 生成device_confidence_tmp表 */
    ConfidenceHelper.buildAllProfileConfidence(spark, sample)

    val profileIdFullTable = "tmp_profileids_full"
    spark.sql(
      s"""
         |select concat(a.profile_id, '_', a.profile_version_id) as profile_id
         |                from t_individual_profile as a
         |                inner join t_profile_version as b
         |                on a.profile_id = b.profile_id and a.profile_version_id = b.profile_version_id
         |                inner join t_profile_metadata as d
         |                on a.profile_id = d.profile_id
         |                inner join t_profile_category as c
         |                on d.profile_category_id = c.profile_category_id
         |                where a.profile_datatype in ('int', 'string', 'boolean', 'double', 'bigint')
         |                  and (profile_table not like '%idfa%' and profile_table not like '%ios%'
         |                       and profile_table not like '%phone%' and profile_table not like '%imei%'
         |                       and profile_table not like '%mac%' and profile_table not like '%serialno%')
         |                  and b.is_avalable = 1 and b.is_visible = 1
         |""".stripMargin).createOrReplaceTempView(profileIdFullTable)



    /** 单体标签(Array[ProfileInfo]) */
    val profiles = FullTagsGeneratorHelper.getProfilesFilterPartition(spark, profileIdFullTable)

    val hiveTables = scala.collection.mutable.ArrayBuffer[HiveTable]()

    val dfArr = profiles.groupBy(_.fullTableName).values.zipWithIndex.flatMap { case (ps, j) =>
      val hiveTable = FullTagsGeneratorHelper.buildHiveTableFromProfiles(spark, sample, ps, tbManager, day)

      val queries: Seq[String] = if (ps.head.isTimewindowTable) {
        //timewindows
        ps.groupBy(_.flagTimewindow).values.zipWithIndex.map{ case (tables, i) =>
          scala.util.Try {
            val twTable = FullTagsGeneratorHelper.buildHiveTableFromProfiles(spark, sample, tables, tbManager, day)
            val featureMapping = s"feature_mapping_${j}_$i"
            /** m = Map("feature=XXX",full_id) */
            val m = FullTagsGeneratorHelper.getValue2IdMapping(tables.filter(_.isTimewindowTable)).toSeq
            val table = tables.head
            /** arr(column) */
            val arr = table.profileColumn.split(";").map(_.trim)
            val dataType = table.profileDataType

            if (table.isFeature) {
              s"""
                 |select ${twTable.key} as device,
                 |  concat('${m.head._2}', '$kvSep', ${FullTagsGeneratorHelper.valueToStr(dataType, arr(0))}) as kv,
                 |  ${twTable.fullUpdateTimeClause(table)} as update_time
                 |from ${twTable.fullTableName}
                 |${twTable.whereClause}
              """.stripMargin
            } else if (table.isTimewindowFlagFeature) {
              spark.createDataset(m)
                .toDF("feature_value", "feature_id")
                .createOrReplaceTempView(featureMapping)

              // 使用inner join来获取对应的id
              s"""
                 |select ${twTable.key} as device,
                 |  concat(b.feature_id, '$kvSep', ${FullTagsGeneratorHelper.valueToStr(dataType, arr(0))}) as kv,
                 |  ${twTable.fullUpdateTimeClause(table)} as update_time
                 |from  ${twTable.fullTableName} as a
                 |inner join $featureMapping as b
                 |on a.feature = b.feature_value
                 |${twTable.whereClause}
              """.stripMargin
            } else {
              s"""
                 |select ${twTable.key} as device,
                 |  concat('${tables.head.fullVersionId}', '$kvSep',
                 |   ${FullTagsGeneratorHelper.valueToStr(dataType, arr(0))}) as kv,
                 |  ${twTable.fullUpdateTimeClause(table)} as update_time
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
          FullTagsGeneratorHelper.processTaglistLikeFields(spark, arr, field)
        }.mkString(",")

        val ps2 = ps.filter(!_.hasTagListField)
        val ps2Columns = FullTagsGeneratorHelper.buildMapStringFromFields(ps2, kvSep)

        val columns = if (ps1.isEmpty) {
          s"concat_ws('$pairSep', $ps2Columns)"
        } else if (ps2Columns.isEmpty) {
          s"concat_ws('$pairSep', $ps1Columns)"
        } else {
          s"concat_ws('$pairSep', $ps2Columns, $ps1Columns)"
        }

        Seq(
          s"""
             |select ${hiveTable.key} as device, $columns as kv
             |     , ${hiveTable.fullUpdateTimeClause(ps.head)} as update_time
             |from   ${hiveTable.fullTableName}
             |${hiveTable.whereClause}
         """.stripMargin
        )
      }

      queries.map(query => sql(spark, query))
    }
//    dfArr.foreach(_.persist(StorageLevel.MEMORY_AND_DISK))
    val df = dfArr.sliding(param.batch, param.batch).map(iter => iter.reduce(_ union _).checkpoint()).reduce(_ union _)
    val unionTable = "unionTable"
    //schema=device,kv,update_time
    df.createOrReplaceTempView(unionTable)

    // 将字符串打成map并去掉map中value为空的kv
    spark.udf.register("cus_str_to_map", FullTagsGeneratorHelper.customUpdateStr2Map _)
    // 去掉没有标签值的标签的置信度
    spark.udf.register("clean_confidence_to_mapArr", FullTagsGeneratorHelper.cleanConfidence2mapArr _)
    // 给每个标签加上updateTime
    spark.udf.register("inset_update_time", FullTagsGeneratorHelper.insertUpdateTime _)
    val confidenceIds = FullTagsGeneratorHelper.getConfidenceProfileIds(spark, profileConfidence)
      .map(t => s"${t._1}_${t._2}")
    val confidenceId2ValueMap =
      s"""
         |map(${confidenceIds.map(fullId => s"'$fullId', tags['$fullId']").mkString(",")})
       """.stripMargin


    sql(spark,
      s"""
         |insert overwrite table ${TablePropUtils.HIVE_TABLE_PROFILE_TAGS_INFO_FULL} partition(day='$day')
         |select device, tags, confidence
         |from
         |  (select device, tags, clean_confidence_to_mapArr(confidence, $confidenceId2ValueMap, '$day') as confidence
         |  from (
         |    select
         |      device,
         |      tags,
         |      str_to_map(confidence, '$pairSep', '$kvSep') confidence
         |    from (
         |      select device,
         |        cus_str_to_map(concat_ws('$pairSep', collect_list(kv)), '$pairSep', '$kvSep') as tags,
         |        max(confidence) as confidence
         |      from (
         |        select device, inset_update_time(kv,update_time) as kv, null as confidence
         |        from $unionTable
         |        union all
         |        select device, null as kv, confidence
         |        from  device_confidence_tmp
         |      ) union_confidence
         |      group by device
         |    ) as a
         |  ) as b
         |) as tmp
         |where tags is not null or confidence is not null

      """.stripMargin)

  }
}
