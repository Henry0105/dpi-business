package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.profilecal.{ProfileBatchMonomerInput, ProfileBatchMonomerOutput,
  ProfileBatchMonomerParam}
import com.mob.dataengine.engine.core.profilecal.ProfileBatchMonomer
import com.mob.dataengine.engine.core.profilecal.ProfileBatchMonomer.ProfileBatchMonomerJob
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite


class ProfileBatchMonomerTest extends FunSuite with LocalSparkSession {
  val json: String = FileUtils.getJson("unittest/profile_cal/profile_cal_batch_monomer.json")
  val profileIds: Seq[String] = Seq("2_1001", "3_1000", "4466_1000", "1034_1000", "7_1000", "4_1000")
  val confidenceIds: Seq[String] = MetadataUtils.findConfidenceProfiles()
  val jobCommon: JobCommon = new JobCommon("jobId", "jobName", "rpcHost", 0, "day")
  val inputUUID: String = "input_uuid"
  val outputUUID: String = "output_uuid"

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._

    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_mobdi_app")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")

    // 创建存储结果表
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE rp_dataengine")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE dm_dataengine_tags")
    spark.sql("create database dm_dataengine_test")
    spark.sql("create database rp_dataengine_test")

    Seq(
      ("d1", "20190401", "cf|4", "input_uuid"),
      ("d2", "20190401", "cf|4", "input_uuid"),
      ("d0", "20190401", "cf|4", "input_uuid")
    ).toDF(
      "data", "created_day", "biz", "uuid"
    ).write.partitionBy("created_day", "biz", "uuid")
      .saveAsTable(PropUtils.HIVE_TABLE_DATA_OPT_CACHE)

    Seq(
      ("p1", Map(4 -> "d1"), 3, 0, "20190102&s1&cf|4", "20190401", "input_uuid2"),
      ("p1", Map(4 -> "d1"), 3, 0, "20190102&s1&cf|4", "20190401", "input_uuid2"),
      ("p2", Map(4 -> "d2"), 3, 0, "20190102&s2&cf|4", "20190401", "input_uuid2"),
      ("p0", Map(4 -> "d0"), 3, 0, "20190102&s0&cf|4", "20190401", "input_uuid2")
    ).toDF(
      "id", "match_ids", "id_type", "encrypt_type", "data", "day", "uuid"
    ).write.partitionBy("day", "uuid")
      .saveAsTable(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)

    val tagsInfoSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_tags/dm_tags_info.sql",
      tableName = PropUtils.HIVE_TABLE_DM_TAGS_INFO)
    createTable(tagsInfoSql)

    val tagsInfoDF = spark.sql(
      s"""
         |select "d1" device, map('3_1000', '97', '4_1000', '1') tags,
         |  map('3_1000', '0.7') confidence, "" update_time, null grouped_tags
         |union all
         |select "d2" device,
         |  map('3_1000', '97', '4_1000', '1', '1034_1000', '1,2', '7_1000', '1') tags,
         |  map('4_1000', '0.1') confidence, "" updated_time, null grouped_tags
         |union all
         |select "d0" device,
         |  map("3_1000", "unknown", "4_1000", "unknown", "1034_1000", "1,2", "7_1000", "1"),
         |  map( "4_1000", "0.1"), "" updated_time, null grouped_tags
       """.stripMargin)
    insertDF2Table(tagsInfoDF, PropUtils.HIVE_TABLE_DM_TAGS_INFO, Some("day=20190408"))
    createView(PropUtils.HIVE_TABLE_DM_TAGS_INFO, "20190408", "day")

    val singleProfileInfoSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/profile/single_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_SINGLE_PROFILE_INFO)
    println(s"now print single profile info $singleProfileInfoSql")
    createTable(singleProfileInfoSql)
    // 初始化属性

    val dataHubSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/profile/single_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_HUB)
    println(s"now print data hub info $dataHubSql")
    createTable(dataHubSql)

    val codeMappingSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_code_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
    println(s"now print code mapping info $codeMappingSql")
    createTable(codeMappingSql)

    prepareCodeMapping()
  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
  }

  test("传入limit为None时,处理不报错") {
    val input = new ProfileBatchMonomerInput("input_uuid", profileIds, inputType = "uuid")
    val output = new ProfileBatchMonomerOutput("output_uuid", "tmp/mono_profile", 1, None)
    val param = new ProfileBatchMonomerParam(Seq(input), output)
    val job = ProfileBatchMonomer.of(spark, jobCommon, param, confidenceIds)
    job.submit()
    val cnt = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_SINGLE_PROFILE_INFO}
         |where uuid = 'output_uuid'
       """.stripMargin).count()
    assertResult(3)(cnt)
  }

  test("处理类似group_list这样的逗号分割的字段,中文匹配问题") {
    val input = new ProfileBatchMonomerInput("input_uuid", profileIds, inputType = "uuid")
    val output = new ProfileBatchMonomerOutput("output_uuid", "tmp/mono_profile", 1, Some(100))
    val param = new ProfileBatchMonomerParam(Seq(input), output)
    val job = ProfileBatchMonomer.of(spark, jobCommon, param, confidenceIds)

    val inputDF = job.getInputDF
    val tagsDF = job.getTagsDF
    val joinedDF = job.getFilteredDF(inputDF, tagsDF)
    val resDF = joinedDF.transform(job.filterByProfileIds)
    val (translatedDF, headers, cvHeaders) = job.translateDF(resDF, param.output.display, profileIds)

    translatedDF.show(false)
    val tags = translatedDF
      .filter("device = 'd2'").select("tags").head()

    val groupList = tags.getMap[String, String](0)("人群细分")
    assert(groupList.split(",").toSet.equals(Set("理财达人", "币圈人士")))
    val house = tags.getMap[String, String](0)("房产")
    assertResult("有")(house)
  }

  test("将dataframe根据splitInfo切分") {
    import spark.implicits._

    val splitInfo = Map(
      "3" -> "2", // 学历
      "4" -> "2", // 未成年子女年龄
      "1034" -> "3", // 人群细分
      "7" -> "5" // 房产
    )

    val input = new ProfileBatchMonomerInput("input_uuid", profileIds, inputType = "uuid")
    val output = new ProfileBatchMonomerOutput("output_uuid", "tmp/mono_profile", 1, Some(100))
    val param = new ProfileBatchMonomerParam(Seq(input), output)
    val job = ProfileBatchMonomer.of(spark, jobCommon, param, confidenceIds)

    val inputDF = job.getInputDF
    val tagsDF = job.getTagsDF
    val joinedDF = job.getFilteredDF(inputDF, tagsDF)
    val resDF = joinedDF.transform(job.filterByProfileIds)
    val splitedDFs = job.splitDF(resDF, splitInfo)

    val res = splitedDFs

    // 切分出4个df,还有一个容错的unknown
    assertResult(4)(res.size)

    assertResult(null)(res("2").filter("device='d2'")
      .select("confidence.3_1000").map(_.getString(0)).collect().head)
    assertResult("97")(res("2").filter("device='d2'")
      .select("tags.3_1000").map(_.getString(0)).collect().head)

    assertResult("0.1")(res("2").filter("device='d2'")
      .select("confidence.4_1000").map(_.getString(0)).collect().head)
    assertResult("1")(res("2").filter("device='d2'")
      .select("tags.4_1000").map(_.getString(0)).collect().head)

    assertResult(null)(res("3").filter("device='d2'")
      .select("confidence.1034_1000").map(_.getString(0)).collect().head)
    assertResult("1,2")(res("3").filter("device='d2'")
      .select("tags.1034_1000").map(_.getString(0)).collect().head)

    assertResult(null)(res("5").filter("device='d2'")
      .select("confidence.7_1000").map(_.getString(0)).collect().head)
    assertResult("1")(res("5").filter("device='d2'")
      .select("tags.7_1000").map(_.getString(0)).collect().head)

    splitedDFs.filter { case (_, df) => df.filter("size(tags) > 0").count() > 0 }
      .foreach { case (categoryId, df) =>
        val profileIds = splitInfo.filter(_._2.equals(categoryId)).keys.toSeq
        val (translatedDF, headers, cvHeaders) = job.translateDF(df, param.output.display, profileIds)
        job.writeToCSV(translatedDF, headers, cvHeaders, job.sep, param.output.hdfsOutput + s"/$categoryId")
      }

    print(s"now show csv df \n ")
    spark.read.options(job.csvOptions).csv(param.output.hdfsOutput + s"/2").show(false)

    val valueD0 = spark.read.options(job.csvOptions).csv(param.output.hdfsOutput + s"/2")
      .filter(row => row.get(0).toString.startsWith("d0")).head.get(0).toString
    assert(!valueD0.contains(s"unknown"))


    val c2Headers = spark.read.options(job.csvOptions).csv(param.output.hdfsOutput + s"/2").schema.map(_.name).head
    assert(c2Headers.contains("学历"))
    assert(c2Headers.contains("未成年子女年龄"))
    assert(!c2Headers.contains("房产"))
    assert(!c2Headers.contains("人群细分"))
  }


  test("将dataframe根据splitInfo切分 display = 0") {
    import spark.implicits._

    val splitInfo = Map(
      "3" -> "2", // 学历
      "4" -> "2", // 未成年子女年龄
      "1034" -> "3", // 人群细分
      "7" -> "5" // 房产
    )

    val input = new ProfileBatchMonomerInput("input_uuid", profileIds, inputType = "uuid")
    val output = new ProfileBatchMonomerOutput("output_uuid", "tmp/mono_profile", 0, Some(100))
    val param = new ProfileBatchMonomerParam(Seq(input), output)
    val job = ProfileBatchMonomer.of(spark, jobCommon, param, confidenceIds)

    val inputDF = job.getInputDF
    val tagsDF = job.getTagsDF
    val joinedDF = job.getFilteredDF(inputDF, tagsDF)
    val resDF = joinedDF.transform(job.filterByProfileIds)
    val splitedDFs = job.splitDF(resDF, splitInfo)

    val res = splitedDFs

    // 切分出4个df,还有一个容错的unknown
    assertResult(4)(res.size)

    assertResult(null)(res("2").filter("device='d2'")
      .select("confidence.3_1000").map(_.getString(0)).collect().head)
    assertResult("97")(res("2").filter("device='d2'")
      .select("tags.3_1000").map(_.getString(0)).collect().head)

    assertResult("0.1")(res("2").filter("device='d2'")
      .select("confidence.4_1000").map(_.getString(0)).collect().head)
    assertResult("1")(res("2").filter("device='d2'")
      .select("tags.4_1000").map(_.getString(0)).collect().head)

    assertResult(null)(res("3").filter("device='d2'")
      .select("confidence.1034_1000").map(_.getString(0)).collect().head)
    assertResult("1,2")(res("3").filter("device='d2'")
      .select("tags.1034_1000").map(_.getString(0)).collect().head)

    assertResult(null)(res("5").filter("device='d2'")
      .select("confidence.7_1000").map(_.getString(0)).collect().head)
    assertResult("1")(res("5").filter("device='d2'")
      .select("tags.7_1000").map(_.getString(0)).collect().head)

    splitedDFs.filter { case (_, df) => df.filter("size(tags) > 0").count() > 0 }
      .foreach { case (categoryId, df) =>
        val profileIdsRename = profileIds.filter(pid =>
          splitInfo.filter(_._2.equals(categoryId)).keys.toSeq.contains(pid.split("_")(0)))
        val (translatedDF, headers, cvHeaders) = job.translateDF(df, param.output.display, profileIdsRename)
        print(s"now show translatedDF \n")
        translatedDF.show(false)
        job.writeToCSV(translatedDF, headers, cvHeaders, job.sep, param.output.hdfsOutput + s"/$categoryId")
      }

    print(s"now show csv df \n ")
    spark.read.options(job.csvOptions).csv(param.output.hdfsOutput + s"/2").show(false)

    val c2Headers = spark.read.options(job.csvOptions).csv(param.output.hdfsOutput + s"/2").schema.map(_.name).head
    assert(c2Headers.contains("3"))
    assert(c2Headers.contains("4"))
    assert(!c2Headers.contains("房产"))
    assert(!c2Headers.contains("人群细分"))
  }

  test("test mysql access") {
    val info = ProfileBatchMonomer.getSplitInfo(Seq("4446_1000", "4445_1000", "4447_1000",
      "4442_1000", "4444_1000", "4443_1000", "3113_1000", "3114_1000", "3115_1000",
      "3116_1000", "3117_1000", "3118_1000", "3119_1000", "3120_1000", "3121_1000",
      "3122_1000", "3123_1000", "3124_1000", "3125_1000", "3126_1000", "3127_1000",
      "3128_1000", "3129_1000", "5343_1000", "5344_1000", "5345_1000", "5346_1000",
      "5347_1000", "5348_1000", "5349_1000", "5350_1000", "5351_1000"
    ))

    assert(info.nonEmpty)
    assertResult(1)(ProfileBatchMonomer.getSplitInfo(Seq("2_1000")).size)
  }

  test("支持sql输出且保留种子包") {
    val uuid =
      s" select device, data from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW} lateral view" +
        " explode(split(match_ids[4], @,@)) t as device" +
        " where uuid = @input_uuid2@ and match_ids[4] is not null "

    val json =
      s"""
         |{
         |    "rpc_port": 0,
         |    "rpc_host": "127.0.0.1",
         |    "day":"20200306",
         |    "jobId":"20200306_test_profile_cal_batch_monomer_sql_keepSeed",
         |    "jobName":"profile_cal_batch_monomer",
         |    "params":[
         |        {
         |            "inputs":[
         |                {
         |                    "inputType":"sql",
         |                    "profileIds":["2_1001", "3_1000", "4466_1000", "1034_1000", "7_1000", "4_1000"],
         |                    "uuid":"$uuid",
         |                    "sep":"^",
         |                    "idx":1,
         |                    "value":null
         |                }
         |            ],
         |            "output":{
         |                "display":1,
         |                "limit":1000,
         |                "module":"date-engine-toolplat",
         |                "uuid":"output_uuid",
         |                "hdfsOutput":"tmp/mono_profile"
         |            }
         |        }
         |    ]
         |}
         |""".stripMargin

    println(json)
    ProfileBatchMonomer.main(Array(json))
    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_SINGLE_PROFILE_INFO}
         |where uuid = 'output_uuid'
       """.stripMargin)


    println("df:")
    df.show(false)
    val cnt = df.count()

    assertResult(4)(cnt)

    val csvOptions = Map("header" -> "true", "quote" -> "\u0000", "sep" -> "\u0001")
    val hdfsOuput = "tmp/mono_profile/0"
    val csvdf = spark.read.options(csvOptions).csv(hdfsOuput)
    csvdf.show(false)

    val csvheader = csvdf.schema.fields(0).name
    println(csvheader)
    assert(csvheader.contains("data") || csvheader.contains("种子包"))
  }

  test("写入的时候也写入了rp_data_hub") {
    ProfileBatchMonomer.main(Array(json))
    val df = sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_SINGLE_PROFILE_INFO}
         |where uuid='$outputUUID'
        """.stripMargin).cache()
    assertResult(3)(df.count())

    val hubDF = sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid='$outputUUID'
        """.stripMargin).cache()
    assertResult(3)(hubDF.count())
  }

  test("可以从rp_data_hub读写数据") {
    import spark.implicits._

    sql(s"alter table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} drop partition(uuid='$inputUUID')")
    sql(s"alter table ${PropUtils.HIVE_TABLE_SINGLE_PROFILE_INFO} drop partition(uuid='$outputUUID')")
    sql(s"alter table ${PropUtils.HIVE_TABLE_DATA_HUB} drop partition(uuid='$outputUUID')")
    val hubSourceDF = sql(
      s"""
         |select stack(2,
         |  map('seed', array('seed1'), '4', array('d1'), '3_1000', array('20200101', '96')),
         |  map('seed', array('seed2'), '4', array('d2'), '2_1000', array('20200101', '96'))
         |) as feature
        """.stripMargin)
    insertDF2Table(hubSourceDF, PropUtils.HIVE_TABLE_DATA_HUB, Some(s"uuid='$inputUUID'"))

    ProfileBatchMonomer.main(Array(json))
    val df = sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_SINGLE_PROFILE_INFO}
         |where uuid='$outputUUID'
        """.stripMargin).cache()
    assertResult(2)(df.count())
    assertResult("97")(df.where("device='d1'")
      .map(_.getAs[Map[String, String]]("tags")).head()("3_1000"))

    val hubDF = sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
         |where uuid='$outputUUID'
        """.stripMargin).cache()
    assertResult(2)(hubDF.count())
    assertResult(Seq("20200101", "96"))(hubDF.where("feature['4'][0]='d1'")
      .map(_.getAs[Map[String, Seq[String]]]("feature")).collect().head("3_1000"))
    assertResult(Seq("seed1"))(hubDF.where("feature['4'][0]='d1'")
      .map(_.getAs[Map[String, Seq[String]]]("feature")).collect().head("seed"))
    assertResult(Seq("20190510", "1"))(hubDF.where("feature['4'][0]='d1'")
      .map(_.getAs[Map[String, Seq[String]]]("feature")).collect().head("4_1000"))

    assertResult(Seq("20190510", "97"))(hubDF.where("feature['4'][0]='d2'")
      .map(_.getAs[Map[String, Seq[String]]]("feature")).collect().head("3_1000"))
    assertResult(Seq("20190510", "1,2"))(hubDF.where("feature['4'][0]='d2'")
      .map(_.getAs[Map[String, Seq[String]]]("feature")).collect().head("1034_1000"))
    assertResult(Seq("seed2"))(hubDF.where("feature['4'][0]='d2'")
      .map(_.getAs[Map[String, Seq[String]]]("feature")).collect().head("seed"))
  }

  test("支持PID金融画像") {
    val mobfinPidLabelFullSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_mobdi_app/mobfin_pid_profile_lable_full_par.sql",
      tableName = "rp_mobdi_app.mobfin_pid_profile_lable_full_par")
    createTable(mobfinPidLabelFullSql)
    spark.read
      .option("header", true)
      .csv("../data/mapping/mobfin_pid_label_full.csv")
      .write.insertInto("rp_mobdi_app.mobfin_pid_profile_lable_full_par")
    createView("rp_mobdi_app.mobfin_pid_profile_lable_full_par",
      "rp_mobdi_app.mobfin_pid_profile_lable_full_par_view")
    spark.table("rp_mobdi_app.mobfin_pid_profile_lable_full_par_view").show(false)

    val uuidIn = "uuid_pid_test_in"
    val uuidOut = "uuid_pid_test_out"

    val cacheDF = sql(
      s"""
         |select stack(4,
         |  13911110001,
         |  13911110002,
         |  13911110003,
         |  13911110020
         |) as data
        """.stripMargin)
    insertDF2Table(cacheDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some(s"created_day='20200105',biz='from_dfs',uuid='$uuidIn'"))
    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).show(false)

    val json =
      s"""
         |{
         |    "rpc_port": 0,
         |    "rpc_host": "127.0.0.1",
         |    "day":"20210105",
         |    "jobId":"test_demo",
         |    "jobName":"profile_cal_batch_monomer",
         |    "params":[
         |        {
         |            "inputs":[
         |                {
         |                    "inputType":"dfs",
         |                    "profileIds":["8122_1000", "8123_1000"],
         |                    "uuid":"$uuidIn",
         |                    "value":"$uuidIn",
         |                    "idType": 3,
         |                    "encrypt": {
         |                      "encryptType":-1
         |                    }
         |                }
         |            ],
         |            "output":{
         |                "display":0,
         |                "limit":1000,
         |                "module":"date-engine-toolplat",
         |                "uuid":"$uuidOut",
         |                "hdfsOutput":"tmp/mono_profile"
         |            }
         |        }
         |    ]
         |}
         |""".stripMargin

    println(json)
    ProfileBatchMonomer.main(Array(json))
    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_SINGLE_PROFILE_INFO}
         |where uuid = '$uuidOut'
       """.stripMargin)


    println("df:")
    df.show(false)
    val cnt = df.count()

    assertResult(4)(cnt)

    val csvOptions = Map("header" -> "true", "quote" -> "\u0000", "sep" -> "\u0001")
    val hdfsOuput = "tmp/mono_profile/0"
    val csvdf = spark.read.options(csvOptions).csv(hdfsOuput)
    csvdf.show(false)
  }

  test("支持多列种子包") {
    val uuidIn = "uuid_pid_test_in"

    val cacheDF = sql(
      s"""
         |select stack(4,
         |  '13911110001,data_1,data_2',
         |  '13911110002,data_1,data_2',
         |  '13911110003,data_1,data_2',
         |  '13911110020,data_1,data_2'
         |) as data
        """.stripMargin)
    insertDF2Table(cacheDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some(s"created_day='20200105',biz='from_dfs',uuid='$uuidIn'"))
    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).where(s"uuid = '$uuidIn'").show(false)

    println(json)
    val inputParam = new ProfileBatchMonomerInput(uuidIn, Seq(), None, Some(1), Some(","), "dfs", 4)
    val param = new ProfileBatchMonomerParam(Seq(inputParam), null)
    val pbm = new ProfileBatchMonomerJob(spark, null, param, null)
    val inputDF = pbm.getInputDF
    inputDF.show(false)

    assertResult(true)(inputDF.collect().map(_.getString(0)).forall(_.forall(_.isDigit)))
  }
}