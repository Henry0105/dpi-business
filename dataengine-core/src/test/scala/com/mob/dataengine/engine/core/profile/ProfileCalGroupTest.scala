package com.mob.dataengine.engine.core.profile


import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.profile.IndividualProfile
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.profilecal.ProfileBatchMonomerParam
import com.mob.dataengine.utils.FileUtils
import com.mob.dataengine.engine.core.profilecal.ProfileCalGroup
import com.mob.dataengine.engine.core.profilecal.ProfileCalGroup.ProfileGroupJob
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, LocalSparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.scalatest.FunSuite


class ProfileCalGroupTest extends FunSuite with LocalSparkSession {
  var metadata: Seq[IndividualProfile] = _
  var jobCommon: JobCommon = _
  var params: Seq[ProfileBatchMonomerParam] = _
  var profileIds: Broadcast[Array[String]] = _
  var confidenceIds: Set[String] = _
  var profileCalGroup: ProfileGroupJob = _
  val PARAMS_KEY = "params"


  private val json = FileUtils.getJson("unittest/profile_cal/profile_cal_group.json")

  def prepare(arg: String): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    jobCommon = JsonMethods.parse(arg)
      .transformField {
        case ("job_id", x) => ("jobId", x)
        case ("job_name", x) => ("jobName", x)
        case ("rpc_host", x) => ("rpcHost", x)
        case ("rpc_port", x) => ("rpcPort", x)
      }
      .extract[JobCommon]
    println(jobCommon)

    params = (JsonMethods.parse(arg) \ PARAMS_KEY)
      .transformField {
        case ("id_type", x) => ("idType", x)
        case ("id_types", x) => ("idTypes", x)
        case ("hdfs_output", x) => ("hdfsOutput", x)
        case ("encrypt_type", x) => ("encryptType", x)
        case ("data_process", x) => ("dataProcess", x)
        case ("profile_ids", x) => ("profileIds", x)
      }
      .extract[Seq[ProfileBatchMonomerParam]]
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_mobdi_app")

    // 创建存储结果表
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_dataengine")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_tags")

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_test")

    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val dataOPTCacheSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)

    val tagsInfoSql = FileUtils.getSqlScript( s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_tags/dm_tags_info.sql",
      tableName = PropUtils.HIVE_TABLE_DM_TAGS_INFO)
    createTable(tagsInfoSql)

    val sourceDF = Seq(
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf0", "20190401", "cf|4", "144409943126089728_5c1873e54ffd7fee159e007f"),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c", "20190401", "cf|4", "144409943126089728_5c1873e54ffd7fee159e007f"),
      ("04399d01c2e2fac798f4f1b47285185ebbdca738", "20190401", "cf|4", "144409943126089728_5c1873e54ffd7fee159e007f")
    ).toDF(
      "data", "created_day", "biz", "uuid"
    )
    insertDF2Table(sourceDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day, biz, uuid"), false)


    val tagsInfoDF = Seq(
      ("cdf7464d664816d83adb983ff45bc4e6bbc70bf0",
        Map("3_1000"->"8", "4_1000"->"1", "7_1000" -> "1",
          "1036_1000"->"cn", "1034_1000" -> "2,3", "2_10001" -> "4"),
        Map("3_1000"->"8"), "20190408"),
      ("04399d01c2e2fac798f4f1b47285185ebbdca738",
        Map("3_1000"->"9", "4_1000"->"0", "7_1000" -> "1",
          "1036_1000"->"cn", "2_1001"-> "3"),
        Map("3_1000"->"9"), "20190408"),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c",
        Map("3_1000"->"8", "4_1000"->"1",
          "1034_1000" -> "1,2", "7_1000" -> "1",
          "1036_1000"->"cn", "2_1001" -> "-1"),
        Map( "4_1000"->"1"), "20190408"),
       ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1e",
          Map("3_1000"->"8", "4_1000"->"1",
            "1034_1000" -> "1,2", "7_1000" -> "1",
            "1036_1000"->"cn", "2_1001" -> "unknown"),
          Map( "4_1000"->"1"), "20190408"),
      ("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1d",
        Map("3_1000"->"8", "4_1000"->"1",
        "1034_1000" -> "1,2", "7_1000" -> "1", "1036_1000"->"ch"),
        Map( "4_1000"->"1"), "20190408")
    ).toDF("device", "tags", "confidence", "day")
    insertDF2Table(tagsInfoDF, PropUtils.HIVE_TABLE_DM_TAGS_INFO, Some("day"), false)

    sql(
      s"""
         |create or replace view ${PropUtils.HIVE_TABLE_DM_TAGS_INFO_VIEW} as
         |select device, tags, confidence
         |from ${PropUtils.HIVE_TABLE_DM_TAGS_INFO}
         |where day = '20190408'
       """.stripMargin)

    val groupProfileInfoSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/profile/group_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_GROUP_PROFILE_INFO)
    spark.sql(
      s"""
         |$groupProfileInfoSql
       """.stripMargin)

    val codeMappingSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_code_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
    println(s"now print code mapping info $codeMappingSql")
    createTable(codeMappingSql)

    prepareCodeMapping()
    // 初始化属性
    init()
  }


  override def afterAll(): Unit = {
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    //    stop()
  }

  def init(): Unit = {
    ProfileCalGroup.main(Array(json))
    prepare(json)
    profileCalGroup = ProfileGroupJob(spark, ProfileCalGroup.jobContext.jobCommon,
      params.head)
    profileCalGroup.submit()
  }


  test(testName = "profile group test") {
    assert(profileCalGroup.jsonDF.count() > 0)
  }

  test("处理类似group_list这样的逗号分割的字段,中文匹配问题") {
    import spark.implicits._

    profileCalGroup.transDF.show(false)

    println (s"display is ${params.head.output.display}")
    val display = params.head.output.display
    var eduCnt: Long = 0
    import spark._
    var tmpDF: DataFrame = emptyDataFrame
    if (display == 0 ) {
      tmpDF = profileCalGroup.transDF
        .filter("label = '3_1000' and label_id = '8'")
    } else {
      tmpDF = profileCalGroup.transDF
        .filter("label = '学历' and label_id = '本科' ")
    }
    eduCnt = tmpDF.select("label", "label_id", "cnt", "percent").head()
      .getAs[Long]("cnt")
    
    print(s"eduCnt is $eduCnt")
    assert(eduCnt == 2.toLong)

    val outputUuid = "144409943126089728_5c1873e54ffd7fee159e007f_chenfq"
    // hive表中人群细分也要分开
    val groupListMap = spark.sql(
      s"""
        |select label_id, cnt
        |from ${PropUtils.HIVE_TABLE_GROUP_PROFILE_INFO}
        |where uuid = '$outputUuid' and label = '${ProfileCalGroup.groupListProfileId}'
      """.stripMargin).map{ r =>
      (r.getString(0), r.getInt(1))
    }.collect().toMap

    assertResult(1)(groupListMap("1"))
    assertResult(2)(groupListMap("2"))
    assertResult(1)(groupListMap("3"))

    val csvOptions: Map[String, String] = Map("header" -> "true", "quote" -> "\u0000",
      "sep" -> "\u0001")
    import spark.implicits._
    val percentSeq = spark.read.options(csvOptions).csv(params.head.output.hdfsOutput)
      .filter(row => row.get(0).toString.startsWith("人群细分"))
      .map(row => row.get(0).toString.split("\t")(3).toDouble).collect()
    assertResult(percentSeq)(Seq(0.6667, 0.3333, 0.3333, 0.3333))

    assertResult(0)(spark.read.options(csvOptions).csv(params.head.output.hdfsOutput)
      .filter(row => row.getString(0).toString.contains(ProfileCalGroup.unknownValue)).count())
  }
}
