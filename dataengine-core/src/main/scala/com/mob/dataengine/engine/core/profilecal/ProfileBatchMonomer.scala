package com.mob.dataengine.engine.core.profilecal

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.enums.DeviceType
import com.mob.dataengine.commons.pojo.{MatchInfo, OutCnt}
import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.utils.{DateUtils, FnHelper, PropUtils}
import com.mob.dataengine.core.utils.DataengineException
import com.mob.dataengine.engine.core.jobsparam.BaseJob
import com.mob.dataengine.engine.core.jobsparam.profilecal.ProfileBatchMonomerParam
import com.mob.dataengine.engine.core.profilecal.helper.{ProfileBatchMonomerJobHelper => Helper}
import com.mob.dataengine.rpc.RpcClient
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.functions.{broadcast, _}
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 批量输出单体画像
 */
object ProfileBatchMonomer extends BaseJob[ProfileBatchMonomerParam] {
  override def run(): Unit = {
    val spark = jobContext.spark

    jobContext.params.foreach { p =>
      val confidenceIds = MetadataUtils.findConfidenceProfiles()
      val profileBatchMonomerJob = of(spark, jobContext.jobCommon, p, confidenceIds)
      profileBatchMonomerJob.submit()
    }
  }

  def of(spark: SparkSession,
         jobCommon: JobCommon,
         param: ProfileBatchMonomerParam,
         confidenceIds: Seq[String]): ProfileBatchMonomerJob = {
    val idType = param.inputIdTypeEnum
    idType match {
      case DeviceType.PHONE =>
        new ProfileBatchMonomerJobWithPId(spark, jobCommon, param, confidenceIds)
      case DeviceType.DEVICE =>
        new ProfileBatchMonomerJob(spark, jobCommon, param, confidenceIds)
      case _ => throw new DataengineException(
        s"[ERROR]目前idType仅支持phone,phone_md5(3),device(4) 输入idType为$idType(${idType.id})")
    }
  }

  class ProfileBatchMonomerJob(
                                spark: SparkSession,
                                jobCommon: JobCommon,
                                param: ProfileBatchMonomerParam,
                                confidenceIds: Seq[String],
                                threshold: Int = 20
                              ) extends ProfileJobCommon(spark, jobCommon, param) {
    val sep: String = "\t"
    val csvOptions: Map[String, String] = Map("header" -> "true", "quote" -> "\u0000",
      "sep" -> "\u0001")

    def submit(): Unit = {
      /** 1.得到idType */
      val inputDF = getInputDF
      /** 2.得到tags */
      val tagsDF = getTagsDF
      /** 3.2个df做inner join (device相同) */
      val joinedDF = getFilteredDF(inputDF, tagsDF)
      /** 4.对 tags和confidence做过滤操作 */
      val resDF = filterByProfileIds(joinedDF).persist(StorageLevel.DISK_ONLY)

      /** 5.导出到hive表中 */
      insertSingleInfo(resDF)
      if (jobCommon.hasRPC()) {
        sendMatchInfo(resDF)
      }

      /** 将结果以csv的格式写入dfs (@see [[profileIds.length > threshold]]时做单独处理) */
      if (profileIds.length > threshold) {
        val splitInfo = getSplitInfo(profileIds)
        val splitedDFs = splitDF(resDF, splitInfo)
          .filter { case (_, df) => df.filter("size(tags) > 0").count() > 0 }

        splitedDFs.foreach { case (categoryId, df) =>
          val profileIdsRename = if (param.output.display == 1) {
            splitInfo.filter(_._2.equals(categoryId)).keys.toSeq
          } else {
            profileIds.filter(pid =>
              splitInfo.filter(_._2.equals(categoryId)).keys.toSeq.contains(pid.split("_")(0)))
          }
          val (translatedDF, headers, cvHeaders) = translateDF(df, param.output.display, profileIdsRename)
          writeToCSV(translatedDF, headers, cvHeaders, sep, param.output.hdfsOutput + s"/$categoryId")
        }
      } else {
        val (translatedDF, headers, cvHeaders) = translateDF(resDF, param.output.display, profileIds)
        writeToCSV(translatedDF, headers, cvHeaders, sep, param.output.hdfsOutput + s"/0")
      }
    }

    /** 过滤掉tags和confidence字段中不在profileIdsBC内的数据 */
    def filterByProfileIds(df: DataFrame): DataFrame = {
      import spark.implicits._
      val flatMapFilterDF = df.flatMap { r =>
        val tags = r.getAs[Map[String, String]]("tags")
        val filteredTags = tags.filter(tag => {
          profileIdsBC.value.contains(tag._1)
        })

        if (filteredTags.isEmpty) {
          None
        } else {
          val device = r.getAs[String]("device")
          val confidence = r.getAs[Map[String, String]]("confidence")
          val data = r.getAs[String]("data")
          val filteredConfidence = if (null == confidence) {
            null
          } else {
            emptyMap2Null(confidence.filter(c => {
              profileIdsBC.value.contains(c._1)
            }))
          }
          Some((data, device, filteredTags, filteredConfidence))
        }
      }.toDF("data", "device", "tags", "confidence").persist(StorageLevel.MEMORY_AND_DISK)
      matchCnt = flatMapFilterDF.count()
      println("tag data ", matchCnt)
      flatMapFilterDF.repartition(FnHelper.getCoalesceNum(matchCnt, 100000))
    }

    def insertSingleInfo(df: DataFrame): Unit = {
      df.createOrReplaceTempView("tmp")
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${PropUtils.HIVE_TABLE_SINGLE_PROFILE_INFO}
           |PARTITION(day=${DateUtils.currentDay()},uuid='${param.output.uuid}')
           |SELECT data, device, tags, confidence
           |FROM tmp
           """.stripMargin)

      writeDataHub()
    }

    def writeDataHub(): Unit = {
      // 合并单体画像的数据
      spark.udf.register("merge_profile", (resultMap: Map[String, String],
                                           seedMap: Map[String, Seq[String]]) => {
        if (null == resultMap || resultMap.isEmpty) {
          seedMap
        } else if (null == seedMap || seedMap.isEmpty) {
          resultMap.map { case (k, v) => (k, Seq(jobCommon.day, v)) }
        } else {
          resultMap.map { case (k, v) => (k, Seq(jobCommon.day, v)) } ++ seedMap
        }
      })

      // 合并种子字段和device字段
      spark.udf.register("merge_feature", (resultMap: Map[String, Seq[String]],
                                           seedMap: Map[String, Seq[String]]) => {
        if (null == resultMap || resultMap.isEmpty) {
          seedMap
        } else if (null == seedMap || seedMap.isEmpty) {
          resultMap
        } else {
          resultMap ++ seedMap
        }
      })

      param.inputs.map { input =>
        spark.sql(
          s"""
             |select feature
             |from ${PropUtils.HIVE_TABLE_DATA_HUB}
             |where uuid = '${input.uuid}'
          """.stripMargin)
      }.reduce(_ union _).createOrReplaceTempView("seed_tb")

      spark.sql(
        s"""
           |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_HUB} partition(uuid='${param.output.uuid}')
           |select merge_feature(map('seed', array(data), '4', array(device)),
           |   merge_profile(tags, feature))
           |from tmp
           |left join (
           |  select seed, feature
           |  from (
           |    select feature['seed'][0] seed, feature,
           |      row_number() over (partition by feature['seed'][0] order by feature['seed'][0]) rn
           |    from seed_tb
           |  ) as a
           |  where rn = 1
           |) as b
           |on tmp.data = b.seed
          """.stripMargin)
    }

    def sendMatchInfo(df: DataFrame): Unit = {
      import spark.implicits._
      val cntDF = df.mapPartitions(iter => {
        val cntArrayMap = scala.collection.mutable.Map[String, Long]()
        for (pid <- profileIdsBC.value) {
          cntArrayMap(pid) = 0.toLong
        }

        iter.foreach(row => {
          val tag = row.getAs[Map[String, String]]("tags")
          if (tag != null) {
            for (pid <- profileIdsBC.value) {
              if (tag.contains(pid) && tag(pid) != null && tag(pid).length > 0) {
                cntArrayMap(pid) += 1
              }
            }
          }
        })

        cntArrayMap.toIterator
      }).toDF("pid", "cnt").cache()

      cntDF.createOrReplaceTempView("cnt_df")

      val pidCntMap = spark.sql("SELECT pid, sum(cnt) AS scnt FROM cnt_df GROUP BY pid")
        .map(row =>
          (row.getAs[String]("pid"), row.getAs[Long]("scnt"))
        ).collect()

      val outCnt = new OutCnt
      pidCntMap.foreach(pidCnt => outCnt.set(s"${pidCnt._1}", pidCnt._2))

      val outUUID = param.output.uuid
      val matchInfo = MatchInfo(jobCommon.jobId, outUUID, inputIDCount, matchCnt, outCnt)
      RpcClient.send(jobCommon.rpcHost, jobCommon.rpcPort,
        s"""2\u0001${param.output.uuid}\u0002${matchInfo.toJsonString()}""")
    }

    /**
     * 将dataframe下的标签根据对应的二级分类切分为多个dataframe
     * 1. tags, confidence两个字段都要切分
     * 2. device字段每个dataframe都要带着
     *
     * @param df        (device, tags, confidence)
     * @param splitInfo 标签id对应的聚类id map(profileid -> groupid)
     * @return map(groupid -> df), 可能会生成为null的map字段
     */
    def splitDF(df: DataFrame, splitInfo: Map[String, String]): Map[String, DataFrame] = {
      // 将map切成多个map
      val splitInfoBC = spark.sparkContext.broadcast(splitInfo)
      spark.udf.register("split_map", (m: Map[String, String]) => {
        if (null == m || m.isEmpty) { // 有些confidence为null
          null
        } else {
          val tmp: Seq[(String, String, String)] = m.toSeq.map { case (k, v) =>
            (splitInfoBC.value.getOrElse(k.substring(0, k.indexOf("_")), "unknown"), k, v)
          }
          tmp.groupBy(_._1).mapValues(seq => seq.map { case (_, k, v) => (k, v) }.toMap)
        }
      })

      val tmpDF = df.selectExpr("data", "device", "split_map(tags) as new_tags",
        "split_map(confidence) as new_confidence").cache()

      (splitInfo.values ++ Seq("unknown")).toSet[String].map { k =>
        (k, tmpDF.selectExpr("data", "device", s"new_tags['$k'] as tags", s"new_confidence['$k'] as confidence"))
      }.toMap
    }

    def translateDF(df: DataFrame, display: Int, profileIds: Seq[String]): (DataFrame, Seq[String], Seq[String]) = {
      if (0 == display) {
        (df, profileIds, confidenceIds)
      } else {
        val transTags = s"trans_tags_${RandomStringUtils.randomAlphanumeric(5)}"
        val transConf = s"trans_conf_${RandomStringUtils.randomAlphanumeric(5)}"
        val profileValueTable = "t_profile_value"
        val profileValueMap = getProfileValueMap(spark, profileValueTable)

        val profileMetadataTable = "t_profile_metadata"
        val profileNameMap = MetadataUtils.findProfileName(profileIds)
        val groupListLikeIds = Set("1034_1000")
        registerTransformUDF(spark, profileNameMap, profileValueMap, transTags, transConf, groupListLikeIds)

        (
          df.selectExpr("data", "device", s"$transTags(tags) tags", s"$transConf(confidence) confidence"),
          profileIds.map(id => profileNameMap.getOrElse(id.split("_").head, id)),
          confidenceIds.map(id => profileNameMap.getOrElse(id.split("_").head, id))
        )
      }
    }

    def writeToCSV(df: DataFrame, headers: Seq[String], cvHeaders: Seq[String],
                   sep: String = "\t", hdfsOutput: String = param.output.hdfsOutput): Unit = {
      val transTable = "trans_table"
      df.createOrReplaceTempView(transTable)
      val buildTsvFn = s"buildTsv_${RandomStringUtils.randomAlphanumeric(5)}"
      registerTSVUDF(spark, buildTsvFn, headers, cvHeaders)
      val tsvHeaderRaw = if (param.output.display == 1) {
        s"设备$sep" + headers.map(h => if (cvHeaders.contains(h)) s"$h$sep${h}_置信度" else h).mkString(sep)
      } else {
        s"device$sep" + headers.map(h =>
          if (cvHeaders.contains(h)) s"$h$sep${h}_confidence" else h).mkString(sep)
      }
      // 保留种子包操作
      val tsvHeader = if (1 == param.output.keepSeed.get) {
        if (param.output.display == 1) s"种子包$sep" + tsvHeaderRaw else s"data$sep" + tsvHeaderRaw
      } else {
        tsvHeaderRaw
      }

      val jsonDF = spark.sql(
        s"""
           |select $buildTsvFn(data, device, tags, confidence) tags
           |from $transTable
           |where tags is not null
           |${if (param.output.limit.nonEmpty) s"limit ${param.output.limit.get}" else ""}
      """.stripMargin).toDF(tsvHeader).cache()

      jsonDF.coalesce(1).write.mode(SaveMode.Overwrite)
        .options(csvOptions)
        .csv(hdfsOutput)
    }

  }


  // 递归地查找分类id对应的1级或2级的分类id
  /**
   * @param map categoryId -> (level, categoryId)
   * @param id  categoryId
   * @return
   */
  @scala.annotation.tailrec
  def queryParent(map: Map[Int, (Int, Int)], id: Int): Int = {
    val array = map.get(id) match {
      case Some(x) => x
      case _ => throw new Exception(s"$id(profile_category_id) is not in table t_profile_category")
    }
    if (array._1 == 1 || array._1 == 2) {
      id
    } else {
      queryParent(map, array._2)
    }
  }

  def registerTSVUDF(spark: SparkSession, fn: String, headers: Seq[String],
                     cvHeaders: Seq[String], sep: String = "\t"): Unit = {
    val headersBC = spark.sparkContext.broadcast(headers)
    val cvHeadersBC = spark.sparkContext.broadcast(cvHeaders)

    spark.udf.register(fn, (data: String, device: String,
                            tag: Map[String, String], confidence: Map[String, String]) => {
      val tagValues = headersBC.value.map { h =>
        val v = tag.getOrElse(h, "")
        if (cvHeadersBC.value.contains(h)) {
          if (null != confidence && confidence.nonEmpty) {
            val cv = confidence.getOrElse(h, "")
            s"$v$sep$cv"
          } else {
            s"$v$sep"
          }
        } else {
          v
        }
      }.mkString(sep)
      s"$data$sep$device$sep$tagValues"
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
                           valueMapping: Map[String, String], transTags: String, transConf: String,
                           groupListLikeIds: Set[String]): Unit = {
    val keyMappingBC = spark.sparkContext.broadcast(keyMapping)
    val valueMappingBC = spark.sparkContext.broadcast(valueMapping)
    val groupListLikeIdsBC = spark.sparkContext.broadcast(groupListLikeIds)
    // valuemapping为: 1_1000_HUAWEI MT7-CL00, 去掉没有中文的字段
    spark.udf.register(transTags, (_m: Map[String, String]) => {
      emptyMap2Null(_m) match {
        case null => null
        case m => emptyMap2Null(
          m.map { case (k, vs) => // 如果没有找到对应的值,就使用原来的值, 处理类似group_list这样的字段
            // k: profile_id + "_" + version_id
            val vv = if (groupListLikeIdsBC.value.contains(k)) {
              vs.split(",").map(v => valueMappingBC.value.getOrElse(s"${k}_$v", v)).mkString(",")
            } else {
              valueMappingBC.value.getOrElse(s"${k}_$vs", vs)
            }
            val vvTrans = if (!List("unknown", "-1").contains(vv.toLowerCase)) vv else "未知"
            keyMappingBC.value(k.substring(0, k.indexOf("_"))) -> vvTrans
          }
        )
      }
    })

    spark.udf.register(transConf, (_m: Map[String, String]) => {
      emptyMap2Null(_m) match {
        case null => null
        case m => emptyMap2Null(
          m.map { case (k, v) => keyMappingBC.value(k.substring(0, k.indexOf("_"))) -> v }
        )
      }
    })
  }

  /**
   * 返回标签id对应的二级分类的id
   *
   * @param profileIds profileId_versionId
   * @return profileId -> categoryId
   */
  def getSplitInfo(profileIds: Seq[String]): Map[String, String] = {
    val profileId2CategoryIdMap = MetadataUtils.findProfileId2CategoryId(profileIds)
    val categoryMap = MetadataUtils.findProfileCategory().map(obj =>
      (obj.id, (obj.level, obj.parentId))).toMap

    val category2ParentMap: Map[String, String] = profileId2CategoryIdMap.values
      .map(catId => (catId.toString, queryParent(categoryMap, catId.toInt).toString)).toMap

    profileId2CategoryIdMap.map { case (profileId, categoryId) =>
      (profileId, category2ParentMap(categoryId))
    }
  }

  class ProfileBatchMonomerJobWithPId(
                                       spark: SparkSession,
                                       jobCommon: JobCommon,
                                       param: ProfileBatchMonomerParam,
                                       confidenceIds: Seq[String],
                                       threshold: Int = 20
                                     )
    extends ProfileBatchMonomerJob(spark, jobCommon, param, confidenceIds, threshold) {

    override def getTagsDF: DataFrame = {
      import spark.implicits._
      Helper.registerNum2Str(spark)
      val dfArr: Array[DataFrame] = Helper.handleTags(spark, param, confidenceIds, Some("profile_table like '%pid%'"))
      // 处理成统一的数据结构
      val schema = StructType(StructField("pid", StringType) ::
        StructField("tags", MapType(StringType, StringType)) ::
        StructField("confidence", MapType(StringType, StringType)) :: Nil)
      if (dfArr.isEmpty) {
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      } else if (dfArr.length == 1) {
        /* 只从一个表中读出的情况,单独处理使其不需要走shuffle */
        val kvStr2mapUDF = udf(Helper.kvStr2map _)
        dfArr.head
          .withColumn("tags", kvStr2mapUDF($"tags"))
          .withColumn("confidence", kvStr2mapUDF($"confidence"))
      } else {
        val kvSeq2mapUDF = udf(Helper.kvSeq2map _)
        dfArr.reduce(_ union _)
          .groupBy("pid")
          .agg(
            kvSeq2mapUDF(collect_list("tags")).as("tags"),
            kvSeq2mapUDF(collect_list("confidence")).as("confidence"))
      }.select($"pid".as(deviceString), $"tags", $"confidence")
        .where($"tags".isNotNull.or($"confidence".isNotNull))
    }

    /**
     * 这里入参可能是phone或者phone_md5
     * phone用pid_decrypt
     * phone_md5用aes_encrypt
     */
    override def getFilteredDF(inputDF: DataFrame, tagsDF: DataFrame): DataFrame = {
      val udfName = if (param.encrypt.isMd5) {
        val aesEncrypt = "aes_encrypt"
        spark.sql(s"create or replace temporary function $aesEncrypt as 'com.mob.udf.AesEncrypt'")
        aesEncrypt
      } else if (param.encrypt.encryptType == -1) {
        // 为了测试使用
        "trim"
      } else {
        val pidEncrypt = "pid_Encrypt"
        spark.sql(s"create or replace temporary function $pidEncrypt as 'com.mob.udf.PidEncrypt'")
        pidEncrypt
      }

      val joinExprs = callUDF(udfName, inputDF(deviceString)) === tagsDF("pid")
      inputIDCount = inputDF.count()
      if (inputIDCount < 10000000) inputDF.cache()
      (if (inputIDCount > 10000000) { // 1kw
        tagsDF.join(inputDF, joinExprs, "right")
      } else {
        tagsDF.join(broadcast(inputDF), joinExprs, "right")
      }).drop(tagsDF("pid"))
    }

    override def filterByProfileIds(df: DataFrame): DataFrame = df

  }

}