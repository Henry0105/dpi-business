package com.mob.dataengine.engine.core.profilecal


import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.pojo.{MatchInfo, OutCnt}
import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.utils.{FnHelper, PropUtils}
import com.mob.dataengine.engine.core.jobsparam.BaseJob
import com.mob.dataengine.engine.core.jobsparam.profilecal.ProfileBatchMonomerParam
import com.mob.dataengine.rpc.RpcClient
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/*
  * 输出群体画像分布
  */
object ProfileCalGroup extends BaseJob[ProfileBatchMonomerParam] {
  @transient private[this] val logger = Logger.getLogger(this.getClass)
  var profileGroupJob: ProfileGroupJob = _
  private val tab: String = "\t"
  val groupListProfileId: String = "1034_1000"
  val unknownValue = "unknown"
  val unknownValueCn = "未知"

  override def run(): Unit = {
    val spark = jobContext.spark

    jobContext.params.foreach { p =>
      val profileGroupJob = ProfileGroupJob(spark, jobContext.jobCommon, p)
      profileGroupJob.submit()
    }
  }

  case class ProfileGroupJob(
    spark: SparkSession,
    jobCommon: JobCommon,
    param: ProfileBatchMonomerParam
  )  extends ProfileJobCommon(spark, jobCommon, param)
    with Serializable {
    @transient var transDF: DataFrame = _
    @transient var jsonDF: DataFrame = _
    val filteredTable = "filtered_table"
    val filteredTableExplode = "filtered_table_explode"
    val dealWithUnknownTable = "deal_with_unknown"
    val finalTable = "final_table"

    def submit() : Unit = {
      initMysql()
      // 这里对device进行去重
      val inputDF = getInputDF.select(deviceString).distinct()
      val tagsDF = getTagsDF
      val tagsFilterDF = secondFilterTagsDF(tagsDF)
      val filteredDF = getFilteredDF(inputDF, tagsFilterDF)
      flatMapFilteredDF(filteredDF)
      explodeFilterDF()
      dealWithUnknownDF()
      insertGroupInfo()
      if (jobCommon.hasRPC()) {
        sendMatchInfo()
      }
      transferDF()
      writeToCSV()
      stop()
    }

    def sql(sql: String): DataFrame = {
      logger.info("\n>>>>>>>>>>>>>>>>>")
      logger.info(sql)
      val df = spark.sql(sql)
      logger.info("<<<<<<<<<<<<<<\n\n")
      df
    }

    def secondFilterTagsDF(tagsDF: DataFrame): DataFrame = {
      val filterCondition = if (param.inputs.head.filter.nonEmpty) {
        param.inputs.head.filter.get.map{filter =>
          s"tags['${filter._1}'] = '${filter._2}'"
        }.mkString(" and ")
      } else " 1=1 "
      tagsDF.createOrReplaceTempView("tags_table")
      sql(
        s"""
           |SELECT device, tags
           |FROM tags_table
           |WHERE $filterCondition
       """.stripMargin)
    }

    def flatMapFilteredDF(filteredDF: DataFrame): Unit = {
      import spark.implicits._
      // map -> 去掉Some()
      val flatMapFilterDF = filteredDF.flatMap{ r =>
        val tags = r.getAs[Map[String, String]]("tags")
        // 为保证各个label下所有label_id的占比之和为1
        val fillTags = scala.collection.mutable.Map[String, String]()
        profileIdsBC.value.foreach{ profileId =>
          if (tags.getOrElse(profileId, null) != null) {
            fillTags += (profileId -> tags(profileId))
          } else {
            fillTags += (profileId -> "-1")
          }
        }

        val device = r.getAs[String]("device")
        Some((device, fillTags.toMap))
      }.toDF(colList: _*).persist(StorageLevel.MEMORY_AND_DISK)
      matchCnt = flatMapFilterDF.count()
      println("matchCnt is ", matchCnt)
      flatMapFilterDF.repartition(FnHelper.getCoalesceNum(matchCnt, 100000))
        .createOrReplaceTempView(filteredTable)
    }

    def explodeFilterDF(): Unit = {
      sql(
        s"""
           |SELECT device, explode(tags) AS (label, label_id)
           |FROM $filteredTable
         """.stripMargin)
        .createOrReplaceTempView(filteredTableExplode)
    }

    def dealWithUnknownDF(): Unit = {

      sql(
        s"""
           |SELECT device, label,
           |CASE WHEN LOWER(CAST(label_id AS string)) in ('unknown', '-1', '未知')
           |  THEN '$unknownValue'
           |  ELSE label_id END AS label_id
           |FROM $filteredTableExplode
         """.stripMargin)
        .createOrReplaceTempView(dealWithUnknownTable)
    }

    def insertGroupInfo(): Unit = {
      import spark.implicits._
      val deviceSum: Long = sql(
        s"""
           |SELECT COUNT(1) AS sum
           |    FROM (
           |        SELECT device
           |        FROM $dealWithUnknownTable
           |        GROUP BY device
           |    ) device_cnt
         """.stripMargin).map(row => row.getAs[Long]("sum")).head

      sql(
        s"""
           |SELECT filter_tbl.label, new_label_id label_id, cnt, $deviceSum AS sum
           |FROM
           |(
           |    SELECT label, new_label_id, count(device) as cnt
           |    FROM $dealWithUnknownTable
           |    LATERAL VIEW EXPLODE (SPLIT(label_id, ',')) s AS new_label_id
           |    GROUP BY label, new_label_id
           |) filter_tbl
         """.stripMargin).createOrReplaceTempView(finalTable)

      sql(
        s"""
           |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_GROUP_PROFILE_INFO}
           |PARTITION(day='${jobCommon.day}',uuid='${param.output.uuid}')
           |SELECT label, label_id, cnt
           |FROM $finalTable
           """.stripMargin)
    }

    def sendMatchInfo(): Unit = {
      val outCnt = new OutCnt
      val outUUID = param.output.uuid
      val matchInfo = MatchInfo(jobCommon.jobId, outUUID, inputIDCount, matchCnt, outCnt)
      RpcClient.send(jobCommon.rpcHost, jobCommon.rpcPort,
        s"""2\u0001${param.output.uuid}\u0002${matchInfo.toJsonString()}""")
    }

    def explodeBeforeTransfer(tableName: String): Unit = {
      sql(
        s"""
           |SELECT label,
           |new_label_id,
           |SUM(cnt) AS cnt,
           |MAX(sum) AS sum
           |FROM $tableName
           |LATERAL VIEW EXPLODE(SPLIT(label_id, ',')) s AS new_label_id
           |GROUP BY label,
           |new_label_id
         """.stripMargin).createOrReplaceTempView("explode_final_tbl")
    }

    def transferDF(): Unit = {
      val (transDFTMP, headersTMP) = if (0 == param.output.display) {
        explodeBeforeTransfer(finalTable)
        val tmp = sql(
          s"""
             |SELECT label,
             |new_label_id AS label_id,
             |cnt,
             |cast(round(cast(cnt as double)/sum, 4) as decimal(5,4)) AS percent
             |FROM explode_final_tbl
         """.stripMargin)
        (tmp, param.inputs.flatMap(_.profileIds))
      } else {
        val transLabel = "trans_labels"
        val transLabelId = "trans_label_id"
        val profileValueTable = "t_profile_value"
        val profileValueMap = getProfileValueMap(spark, profileValueTable)

        val profileNameMap = MetadataUtils.findProfileName(profileIdsBC.value)
        val groupListLikeIds = Set(groupListProfileId)
        registerTransformUDF(spark, profileNameMap, profileValueMap,
          transLabel, transLabelId, groupListLikeIds)

        sql(
          s"""
             |select label, if(label_id = '$unknownValue', '$unknownValueCn', label_id) label_id,
             |  cnt, sum
             |from (
             |  SELECT $transLabel(label) AS label,
             |    $transLabelId(label, label_id) AS label_id,
             |    cnt,
             |    sum
             |  FROM $finalTable
             |) as final_cn
           """.stripMargin).createOrReplaceTempView(s"${finalTable}_explode")

        explodeBeforeTransfer(s"${finalTable}_explode")

        val tmp = sql(
          s"""
             |SELECT label,
             |new_label_id AS label_id,
             |cnt,
             |cast(round(cast(cnt as double)/sum, 4) as decimal(5,4)) AS percent
             |FROM explode_final_tbl
         """.stripMargin)

        (tmp, param.inputs.flatMap(_.profileIds).map(id => profileNameMap(id.split("_").head)))
      }

      transDF = transDFTMP
    }

    def writeToCSV(): Unit = {
      val transTable = "trans_table"
      transDF.createOrReplaceTempView(transTable)
      val buildTsvFn = "buildTsv"
      registerTSVUDF(spark, buildTsvFn)

      val tsvHeader = if (param.output.display == 1) {
        s"标签${tab}标签值${tab}设备量${tab}占比"
      } else {
        s"label${tab}label_id${tab}cnt${tab}percent"
      }

      val orderSQLString = if (param.output.order.nonEmpty) {
        val orderValue = if (param.output.order.get.head("asc") == 1) "asc" else "desc"
        s"""SELECT label, label_id, cnt, percent,
            row_number() over (partition by label order by percent $orderValue) as percent_num
          FROM $transTable""".stripMargin
      } else s"""SELECT label, label_id, cnt, percent FROM $transTable ORDER BY label, percent"""

      val orderString = if (param.output.order.nonEmpty) {
        s"""ORDER BY label, percent_num asc""".stripMargin
      } else s"""ORDER BY label, percent"""

      jsonDF = sql(
        s"""
           |SELECT $buildTsvFn(label, label_id, cnt, cast(percent as string))
           |FROM ( $orderSQLString ) tmp
           |$orderString
      """.stripMargin).toDF(tsvHeader).cache()

      val csvOptionsMap = Map(
        "header" -> "true",
        "quote" -> "\u0000",
        "sep" -> "\u0001"
      )
      jsonDF.coalesce(1)
        .write.mode(SaveMode.Overwrite).options(csvOptionsMap)
        .csv(param.output.hdfsOutput)
    }

    def stop(): Unit = {
      transDF.unpersist()
      jsonDF.unpersist()
    }

  }

  def registerTSVUDF(spark: SparkSession, fn: String): Unit = {
    spark.udf.register(fn,
      (label: String, label_id: String, cnt: Int, percent: String) => {
      s"$label$tab$label_id$tab$cnt$tab$percent"
    })
  }

  def emptyMap2Null(m: Map[String, String]): Map[String, String] = {
    if (null == m || m.isEmpty) {
      null
    } else {
      m
    }
  }


  // keyMapping key:  profile_id
  // valueMappingBC key:  profile_id + "_" + version_id + "_" + value
  def registerTransformUDF(spark: SparkSession, keyMapping: Map[String, String],
    valueMapping: Map[String, String], transLabel: String, transLabelId: String,
    groupListLikeIds: Set[String]): Unit = {
    val keyMappingBC = spark.sparkContext.broadcast(keyMapping)
    val valueMappingBC = spark.sparkContext.broadcast(valueMapping)
    val groupListLikeIdsBC = spark.sparkContext.broadcast(groupListLikeIds)
    // value mapping为: 1_1000_HUAWEI MT7-CL00, 去掉没有中文的字段
    spark.udf.register(transLabel, (col_name: String) => {
      // 如果没有找到对应的值,就使用原来的值, 处理类似group_list这样的字段
      // k: profile_id + "_" + version_id
      keyMappingBC.value(col_name.substring(0, col_name.indexOf("_")))
    })

    spark.udf.register(transLabelId, (label: String, labelId: String) => {
      emptyMap2Null(Map(label -> labelId)) match {
        case null => null
        case m => emptyMap2Null(
          m.map { case (k, vs) =>  // 如果没有找到对应的值,就使用原来的值, 处理类似group_list这样的字段
            // k: profile_id + "_" + version_id
            val vv = if (groupListLikeIdsBC.value.contains(k)) {
              vs.split(",").map(v => valueMappingBC.value.getOrElse(s"${k}_$v", v)).mkString(",")
            } else {
              valueMappingBC.value.getOrElse(s"${k}_$vs", vs)
            }
            keyMappingBC.value(k.substring(0, k.indexOf("_"))) -> vv
          }
        ).values.head
      }
    })

  }

}
