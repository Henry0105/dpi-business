package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.enums.DeviceType
import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.commons.utils.{Md5Helper, PropUtils}
import com.mob.dataengine.commons.{Encrypt, JobCommon}
import com.mob.dataengine.engine.core.jobsparam.{IdMappingV4Input, IdMappingV4Output, IdMappingV4Param, JobContext2}
import com.mob.dataengine.engine.core.mapping.IdMappingV4
import com.mob.dataengine.utils.FileUtils
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.LocalSparkSession
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization._
import org.scalatest.FunSuite
import IdMappingV4.{PID_DECRYPT, PID_ENCRYPT}


/**
 * @author juntao zhang
 */
class IdMappingV4Test extends FunSuite with LocalSparkSession with UDFCollections {
  implicit val formats: DefaultFormats.type = DefaultFormats
  val json: String = FileUtils.getJson("unittest/mapping/id_mapping_v4.json")

  val jobCommon: JobCommon = new JobCommon("jobId", "jobName", "rpcHost", 0, "day")

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")

    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database dm_dataengine_tags")
    spark.sql("create database rp_dataengine")
    spark.sql("create database dm_dataengine_test")
    spark.sql("create database rp_dataengine_test")

    val dataOPTCacheNewSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val dataOPTCacheSql = FileUtils.getSqlScript(s"conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)
//
//    val imeiMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
//      s"dm_tables_create/dm_dataengine_mapping/dm_imei_mapping.sql",
//      tableName = PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3)
//    createTable(imeiMappingSql)
//
//    val macMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
//      s"dm_tables_create/dm_dataengine_mapping/dm_mac_mapping.sql",
//      tableName = PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3)
//    createTable(macMappingSql)

    val deviceMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_dataengine_mapping_sec.sql",
      tableName = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC_VIEW)
    createTable(deviceMappingSql)

//    val tagsInfoSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
//      s"dm_tables_create/dm_dataengine_tags/dm_tags_info.sql",
//      tableName = PropUtils.HIVE_TABLE_DM_TAGS_INFO)
//    createTable(tagsInfoSql)

//    val oaidMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
//      s"dm_tables_create/dm_dataengine_mapping/dm_oaid_mapping.sql",
//      tableName = PropUtils.HIVE_TABLE_DM_OAID_MAPPING_V3)
//    createTable(oaidMappingSql)

    val ieidMappingViewSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_dataengine_mapping_sec.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IEID_MAPPING_VIEW)
    createTable(ieidMappingViewSql)

    val mcidMappingViewSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_dataengine_mapping_sec.sql",
      tableName = PropUtils.HIVE_TABLE_DM_MCID_MAPPING_VIEW)
    createTable(mcidMappingViewSql)

//    val deviceMappingViewSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
//      s"dm_tables_create/dm_dataengine_mapping/dm_device_mapping.sql",
//      tableName = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3_VIEW)
//    createTable(deviceMappingViewSql)

    val ifidMappingViewSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_dataengine_mapping_sec.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IFID_MAPPING_VIEW)
    createTable(ifidMappingViewSql)

//    val oaidMappingViewSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
//      s"dm_tables_create/dm_dataengine_mapping/dm_oaid_mapping.sql",
//      tableName = PropUtils.HIVE_TABLE_DM_OAID_MAPPING_V3_VIEW)
//    createTable(oaidMappingViewSql)

    val tagsInfoViewSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_tags/dm_tags_info.sql",
      tableName = PropUtils.HIVE_TABLE_DM_TAGS_INFO_VIEW)
    createTable(tagsInfoViewSql)

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

    val phoneBkMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_pid_mapping_bk.sql",
      tableName = PropUtils.HIVE_TABLE_DM_PID_MAPPING_BK)
    createTable(phoneBkMappingSql)

    val dmPidMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_mapping_sec.sql", PropUtils.HIVE_TABLE_DM_PID_MAPPING_VIEW)
    createTable(dmPidMappingSql)

    prepareCodeMapping()

//    sql(s"drop function IF EXISTS $PID_ENCRYPT")
//    sql(s"drop function IF EXISTS $PID_DECRYPT")
    sql(s"create or replace temporary function $PID_ENCRYPT as 'com.mob.udf.PidEncrypt'")
    sql(s"create or replace temporary function $PID_DECRYPT as 'com.mob.udf.PidDecrypt'")
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")
  }

  def prepareWarehouse(): Unit = {
    val dataOptDF = Seq(
      "a0000037ddf832,20190201", // eefabbbf08607477ca149109f4ce05ae
      "a0000037de0616,20190301", // 54e49e2cf8a9a87a5195cd241415e4a6
      "a0000037de087f,20190210", // e1fbe197320b938b414ec37052c1a1ac
      "a0000037de1b3c,20190210", // c73381232cb365ffaf52d74f7c073ecb
      "a0000037de1bsc,20190213"  // 60b33c07f3da8f9756a812c0d74af7ef
    ).toDF("data")

    insertDF2Table(dataOptDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20190101', biz='3|1', uuid='11328a45f49e6c545b715a1ad2831af0'"))

    val ieidDeviceDF = spark.sql(
      s"""
         |select "eefabbbf08607477ca149109f4ce05ae" ieid, array("cdf7464d664816d83adb983ff45bc4e6bbc70bf0") device,
         |  array("20150731") device_ltm
         |union all
         |select "54e49e2cf8a9a87a5195cd241415e4a6" ieid, array("bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c") device,
         |  array("20150731") device_ltm
         |union all
         |select "e1fbe197320b938b414ec37052c1a1ac" ieid, array("04399d01c2e2fac798f4f1b47285185ebbdca738") device,
         |  array("20150731") device_ltm
         |union all
         |select "c73381232cb365ffaf52d74f7c073ecb" ieid, array("eeea9b339baf5d7037f8135ce36c57aadca9c661",
         |"340dd1242a75d8b596175d7c17f552c09cd6ce6e", "340dd1242a75d8b596175d7c17f552c09cd6ce6f") device, array
         |("20160731", "20151031", "20151231") device_ltm
       """.stripMargin).toDF("ieid", "device", "device_ltm")
    insertDF2Table(ieidDeviceDF, PropUtils.HIVE_TABLE_DM_IEID_MAPPING_VIEW, Some("day=20190320, plat=1"))

    val tagsInfoDF = spark.sql(
      s"""
         |select "cdf7464d664816d83adb983ff45bc4e6bbc70bf0" device, map('1_1000', 'a') tags
         |union all
         |select "bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c" device, map('1_1000', 'a') tags
         |union all
         |select "04399d01c2e2fac798f4f1b47285185ebbdca738" device, map('1_1000', 'a') tags
         |union all
         |select "eeea9b339baf5d7037f8135ce36c57aadca9c661" device, map('1_1000', 'a', '2_1000', 'b') tags
         |union all
         |select "340dd1242a75d8b596175d7c17f552c09cd6ce6e" device, map('1_1000', 'a') tags
         |union all
         |select "340dd1242a75d8b596175d7c17f552c09cd6ce6f" device, null tags
       """.stripMargin)
    insertDF2Table(tagsInfoDF, PropUtils.HIVE_TABLE_DM_TAGS_INFO_VIEW, Some("day=20190101"))
  }

  test("idmappingv4 明文ieid匹device") {
    prepareWarehouse()
    IdMappingV4.main(Array(json))

    val resDF = spark.sql(
      s"""
         |select id, match_ids
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid'
        """.stripMargin)

    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW).show(false)

    assertResult(5)(resDF.count())
    assertResult(4)(resDF.where("match_ids is not null").count())

    // map中对应的value为空,则去掉该entry
    // a0000037de1bsc 对应的device为,match_ids应该为null
    val nullCnt = spark.sql(
      s"""
         |select id
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid' and id= 'a0000037de1bsc' and match_ids is null
        """.stripMargin).count()

    assertResult(1)(nullCnt)
  }

  test("idmappingv4 匹配输出多条 明文ieid匹device") {
    prepareWarehouse()
    IdMappingV4.main(Array(json))

    val res = spark.sql(
      s"""
         |select match_ids[4] devices
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid' and id = 'a0000037de1b3c'
       """.stripMargin).map(_.getString(0)).collect().head.split(",")

    assert(res.size == res.intersect(Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6f")).length)

    val matchInfo = IdMappingV4.jobContext.matchInfo
    assertResult(4)(matchInfo.matchCnt)
    assertResult(5)(matchInfo.idCnt)
    assertResult(5)(matchInfo.outCnt.m("device_cnt"))
  }

  test("idmappingv4 默认匹配1条 明文ieid匹device") {
    prepareWarehouse()
    val newJson = writePretty(JsonMethods.parse(json).removeField {
      case ("matchLimit", x) => true
      case _ => false
    })

    IdMappingV4.main(Array(newJson))
    val res = spark.sql(
      s"""
         |select match_ids[4] devices
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid' and id = 'a0000037de1b3c'
       """.stripMargin).map(_.getString(0)).collect().head.split(",")

    assert(res.size == res.intersect(Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661")).length)

    val matchInfo = IdMappingV4.jobContext.matchInfo
    assertResult(4)(matchInfo.matchCnt)
    assertResult(5)(matchInfo.idCnt)
    assertResult(4)(matchInfo.outCnt.m("device_cnt"))
  }

  test("idmappingv4 match_limit=-1 匹配所有 明文ieid匹device") {
    prepareWarehouse()
    val newJson = writePretty(JsonMethods.parse(json).mapField {
      case ("matchLimit", x) => ("matchLimit", -1)
      case x => x
    })

    IdMappingV4.main(Array(newJson))

    val res = spark.sql(
      s"""
         |select match_ids[4] devices
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'it_uuid' and id = 'a0000037de1b3c'
       """.stripMargin).map(_.getString(0)).collect().head.split(",")

    assert(res.size == res.intersect(Seq("eeea9b339baf5d7037f8135ce36c57aadca9c661",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6e",
      "340dd1242a75d8b596175d7c17f552c09cd6ce6f")).length)

    val matchInfo = IdMappingV4.jobContext.matchInfo
    assertResult(4)(matchInfo.matchCnt)
    assertResult(5)(matchInfo.idCnt)
    assertResult(6)(matchInfo.outCnt.m("device_cnt"))
  }


  test("种子数据用逗号分割,ieid匹配ieid") {
    prepareWarehouse()
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
         |  uuid='uuid1')
         |select '351890081076845' as data
         |union all
         |select '866383025076723' as data
         |union all
         |select '351890081294786' as data
       """.stripMargin)

    val input = new IdMappingV4Input(inputType = "uuid", uuid = "uuid1", idType = 1, header = 1,
      sep = Some(","), idx = Some(1), headers = Some(Seq("in_0")))
    val output = new IdMappingV4Output(uuid = "uuid_output_jiraid", idTypes = Seq(1),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}")
    val param = new IdMappingV4Param(Seq(input), output)

    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val nullCnt = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'uuid_output_jiraid' and match_ids is null
        """.stripMargin).count()

    assertResult(3)(nullCnt)
  }


  test("种子数据用逗号分割,第二列为id列,且为mcid,输出的df是对mcid做处理的 输出mcid") {
    val src = spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|2',
         |  uuid='uuid298')
         |select 'pid,id,label' as data
         |union all
         |select '1,aa:bb:cc,3' as data
         |union all
         |select '1,aabbCC,4' as data
         |union all
         |select '1,aa:BB:cc,B' as data
         |union all
         |select '1,aa:ee:cc,B' as data
       """.stripMargin)

    val srcDF = spark.sql(
      s"""
         |select md5('aa:bb:cc') mcid, split('d1,d2', ',') device, split('1,2', ',') device_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_MCID_MAPPING_VIEW, Some("day='20190320'"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = "uuid298", idType = 2, header = 1,
      sep = Some(","), idx = Some(2), headers = Some(Seq("in_pid", "in_id", "in_label")))
    val output = new IdMappingV4Output(uuid = "out_uuid", idTypes = Seq(2, 3, 4), hdfsOutput = "/tmp/test_idmapping_V4")
    val param = new IdMappingV4Param(Seq(input), output)
    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'out_uuid'
       """.stripMargin)
    df.show(false)
    assertResult("d2")(df.filter("id='aa:bb:cc'")
      .select(col("match_ids.4")).map(_.getString(0)).head)
    assertResult("d2")(df.filter("id='aabbCC'")
      .select(col("match_ids.4")).map(_.getString(0)).head)
  }

  test("种子数据只有1列,检测落地到hive的数据 device匹配ieid,mcid,ifid") {
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
         |  uuid='uuid453')
         |select 'd1' as data
         |union all
         |select 'd2' as data
         |union all
         |select 'd3' as data
         |union all
         |select 'd4' as data
       """.stripMargin)
    val srcDF = sql(
      s"""
         |select 'd1' as device, split('e1,e2', ',') as ieid, split('1,2', ',') as ieid_ltm,
         |  split('m1,m2', ',') as mcid, split('1,2', ',') as mcid_ltm,
         |  split('if1,if2', ',') as ifid, split('1,2', ',') as ifid_ltm
         |union all
         |select 'd2' as device, split('e1,e2', ',') as ieid, split('1,2', ',') as ieid_ltm,
         |  split('m1,m2', ',') as mcid, split('1,2', ',') as mcid_ltm,
         |  split('if1,if2', ',') as ifid, split('1,2', ',') as ifid_ltm
         |union all
         |select 'd3' as device, split('e1,e2', ',') as imei, split('1,2', ',') as imei_ltm,
         |  split('m1,m2', ',') as mcid, split('1,2', ',') as mcid_ltm,
         |  split('if1,if2', ',') as ifid, split('1,2', ',') as ifid_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC_VIEW, Some("day='20190110', plat=1"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = "uuid453", idType = 4)
    val output = new IdMappingV4Output(uuid = "uuid474", idTypes = Seq(1, 2, 7), encrypt = Encrypt(1),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}")
    val param = new IdMappingV4Param(Seq(input), output)
    val jc = JobContext2(spark, jobCommon, param)
    IdMappingV4.run(jc)

    val df = sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'uuid474'
       """.stripMargin)

    df.show(false)

    assertResult("e2")(df.filter("id='d1'")
      .select(col("match_ids.1")).map(_.getString(0)).head)
    assertResult("m2")(df.filter("id='d1'")
      .select(col("match_ids.2")).map(_.getString(0)).head)
    assertResult("if2")(df.filter("id='d1'")
      .select(col("match_ids.7")).map(_.getString(0)).head)
    assertResult(3)(jc.matchInfo.matchCnt)
    assertResult(4)(jc.matchInfo.idCnt)
    assertResult(1)(jc.matchInfo.outCnt.m("ifid_cnt"))
  }

  test("种子数据有3列,第二列为要匹配的列,输出做md5加密,检测落地到hive的数据") {
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
         |  uuid='it_is_uuid')
         |select 'c1,device,c2' as data
         |union all
         |select '1,d1,a' as data
         |union all
         |select '2,d2,b' as data
         |union all
         |select '3,d3,c' as data
       """.stripMargin)
    val srcDF = spark.sql(
      s"""
         |select 'd1' as device, array(md5('e1'), md5('e2')) as ieid, split('1,2', ',') as ieid_ltm,
         |  array(md5('m1'), md5('m2')) as mcid, split('1,2', ',') as mcid_ltm
         |union all
         |select 'd2' as device, array(md5('e1'), md5('e2')) as ieid, split('1,2', ',') as ieid_ltm,
         |  array(md5('m1'), md5('m2')) as mcid, split('1,2', ',') as mcid_ltm
         |union all
         |select 'd3' as device, array(md5('e1'), md5('e2')) as ieid, split('1,2', ',') as ieid_ltm,
         |  array(md5('m1'), md5('m2')) as mcid, split('1,2', ',') as mcid_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_SEC_VIEW, Some("day='20190110', plat=1"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = "it_is_uuid", idType = 4, sep = Some(","), header = 1,
      idx = Some(2), headers = Some(Seq("in_c1", "in_device", "in_c2")))
    val output = new IdMappingV4Output(uuid = "out_uuid", idTypes = Seq(1, 2, 4),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(1))
    val param = new IdMappingV4Param(Seq(input), output)
    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'out_uuid'
       """.stripMargin)

    df.show(false)

    assertResult("68a9e49bbc88c02083a062a78ab3bf30")(df.filter("id='d1'")
      .select(col("match_ids.1")).map(_.getString(0)).head)
    assertResult("aaf2f89992379705dac844c0a2a1d45f")(df.filter("id='d1'")
      .select(col("match_ids.2")).map(_.getString(0)).head)
    assertResult("d1")(df.filter("id='d1'")
      .select(col("match_ids.4")).map(_.getString(0)).head)
  }


  test("输入为md5加密, 利用md5字段join") {
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
         |  uuid='it_is_uuid')
         |select md5('ieid') as data
         |union all
         |select md5('e1md5') as data
         |union all
         |select md5('e2md5') as data
         |union all
         |select md5('e3md5') as data
       """.stripMargin)
    val srcDF = spark.sql(
      s"""
         |select md5('e1md5') ieid, split('d1,d2', ',') device, split('1,2', ',') device_ltm,
         |  split('m1md5,m2md5,m3md5', ',') mcid, split('1,2,3', ',') mcid_ltm,
         |  split('s1,s2,', ',') snid, split('1,2', ',') snid_ltm
         |union all
         |select md5('e2md5') ieid, split('d1,d2', ',') device, split('1,2', ',') device_ltm,
         |  split('m1md5,m2md5,m3md5', ',') mcid, split('1,2,3', ',') mcid_ltm,
         |  split('s1,s2,', ',') snid, split('1,2', ',') snid_ltm
         |union all
         |select md5('e3md5') ieid, split('d1,d2', ',') device, split('1,2', ',') device_ltm,
         |  split('m1md5,m2md5,m3md5', ',') md5, split('1,2,3', ',') mcid_ltm,
         |  split('s1,s2,', ',') snid, split('1,2', ',') snid_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_IEID_MAPPING_VIEW, Some("day='20190320', plat=1"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = "it_is_uuid", idType = 1, sep = Some(","),
      header = 1, encrypt = Encrypt(1), idx = Some(1), headers = Some(Seq("in_ieid")))
    val output = new IdMappingV4Output(uuid = "out_uuid", idTypes = Seq(2, 4, 9), matchLimit = Some(3),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(1))
    val param = new IdMappingV4Param(Seq(input), output)
    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val df = sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'out_uuid'
       """.stripMargin)

    df.show(false)

    assertResult("s2,s1")(
      df.filter("id=md5('e1md5')")
      .select(col("match_ids.9")).map(_.getString(0)).head)
    assertResult("m3md5,m2md5,m1md5")(df.filter("id=md5('e1md5')")
      .select(col("match_ids.2")).map(_.getString(0)).head)
    assertResult("d2,d1")(df.filter("id=md5('e1md5')")
      .select(col("match_ids.4")).map(_.getString(0)).head)
  }

//  test("idmapping匹到多个device的时候,取画像最多的device") {
//    prepareWarehouse()
//    val newJson: String = FileUtils.getJson("unittest/mapping/id_mapping_v3_device_match.json")
//    IdMappingV3.main(Array(newJson))
//
//    val res = spark.sql(
//      s"""
//         |select match_ids[4] devices
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = 'it_uuid' and id = 'a0000037de1b3c'
//       """.stripMargin).map(_.getString(0)).collect().head.split(",")
//
//    assertResult(1)(res.size)
//    assertResult("eeea9b339baf5d7037f8135ce36c57aadca9c661")(res.head)
//
//    val matchInfo = IdMappingV3.idmappingV3Job.matchInfo
//    assertResult(4)(matchInfo.matchCnt)
//    assertResult(5)(matchInfo.idCnt)
//    assertResult(5)(matchInfo.outCnt.m("device_cnt"))
//  }


  test("输入为ifid, 输出device mcid pid的md5") {
    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
         |  uuid='$inputUUID')
         |select '1234567890ABCD' as data
         |union all
         |select '2234567890ABCD' as data
         |union all
         |select '3234567890ABCD' as data
       """.stripMargin)

    val if1 = Md5Helper.entryMD5_32("1234567890ABCD")
    val if2 = Md5Helper.entryMD5_32("2234567890ABCD")
    val if3 = Md5Helper.entryMD5_32("3234567890ABCD")

    // pid1
    val pid1 = "ab31905ce4745d21acb2d093603073081cdeb41c53473b0d3055327277fc7d02f654ec6b3bef10f5c27ed5e9fb5c37cc"
    // pid2
    val pid2 = "432c516c44aa7806303f0416dfd6ea5d23288f99b7c7881f19e2b35768aa60b9f654ec6b3bef10f5c27ed5e9fb5c37cc"

    val srcDF = sql(
      s"""
         |select '$if1' ifid, array('d1') device, array('1') device_ltm, 'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$if2' ifid, array('d2') device, array('1') device_ltm, 'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$if3' ifid, array('d3') device, array('1') device_ltm, 'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_IFID_MAPPING_VIEW, Some("day='20190110', plat=1"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = inputUUID, idType = 7, header = 0)
    val output = new IdMappingV4Output(uuid = outputUUID, idTypes = Seq(4, 2, 3),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(1))
    val param = new IdMappingV4Param(Seq(input), output)
    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = '$outputUUID'
       """.stripMargin)
    df.show(false)
    assertResult("d3")(df.filter(s"id='3234567890ABCD'")
      .select(col("match_ids.4")).map(_.getString(0)).head)
    assertResult("mcid2")(df.filter(s"id='3234567890ABCD'")
      .select(col("match_ids.2")).map(_.getString(0)).head)
    assertResult(s"$pid2")(df.filter(s"id='3234567890ABCD'")
      .select(col("match_ids.3")).map(_.getString(0)).head)
  }

  test("输入为ifid, 输出pid") {
    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
         |  uuid='$inputUUID')
         |select '1234567890ABCD' as data
         |union all
         |select '2234567890ABCD' as data
         |union all
         |select '3234567890ABCD' as data
       """.stripMargin)

    val if1 = Md5Helper.entryMD5_32("1234567890ABCD")
    val if2 = Md5Helper.entryMD5_32("2234567890ABCD")
    val if3 = Md5Helper.entryMD5_32("3234567890ABCD")

    // pid1
    val pid1 = "ab31905ce4745d21acb2d093603073081cdeb41c53473b0d3055327277fc7d02f654ec6b3bef10f5c27ed5e9fb5c37cc"
    // pid2
    val pid2 = "432c516c44aa7806303f0416dfd6ea5d23288f99b7c7881f19e2b35768aa60b9f654ec6b3bef10f5c27ed5e9fb5c37cc"

    val srcDF = sql(
      s"""
         |select '$if1' ifid, array('d1') device, array('1') device_ltm, 'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$if2' ifid, array('d2') device, array('1') device_ltm, 'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$if3' ifid, array('d3') device, array('1') device_ltm, 'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_IFID_MAPPING_VIEW, Some("day='20190110', plat=1"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = inputUUID, idType = 7, header = 0)
    val output = new IdMappingV4Output(uuid = outputUUID, idTypes = Seq(3),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(5))
    val param = new IdMappingV4Param(Seq(input), output)
    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = '$outputUUID'
       """.stripMargin)
    df.show(false)
    assertResult(s"$pid2")(df.filter(s"id='3234567890ABCD'")
      .select(col("match_ids.3")).map(_.getString(0)).head)
  }

  test("输入为pid的MD5, 输出device") {
    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
         |  uuid='$inputUUID')
         |select md5('pid1') as data
         |union all
         |select md5('pid2') as data
       """.stripMargin)

    // pid1
    val pid1 = "ab31905ce4745d21acb2d093603073081cdeb41c53473b0d3055327277fc7d02f654ec6b3bef10f5c27ed5e9fb5c37cc"
    // pid2
    val pid2 = "432c516c44aa7806303f0416dfd6ea5d23288f99b7c7881f19e2b35768aa60b9f654ec6b3bef10f5c27ed5e9fb5c37cc"

    val srcDF = sql(
      s"""
         |select '$pid1' pid, array('d1') device, array('1') device_ltm, 'e1_tm' update_time,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$pid2' pid, array('d2') device, array('1') device_ltm, 'e1_tm' update_time,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_PID_MAPPING_VIEW, Some("day='20190110'"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = inputUUID, idType = 3, header = 0)
    val output = new IdMappingV4Output(uuid = outputUUID, idTypes = Seq(4, 2),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(1))
    val param = new IdMappingV4Param(Seq(input), output)
    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = '$outputUUID'
       """.stripMargin)
    df.show(false)
    assertResult("d1")(df.filter("id=md5('pid1')")
      .select(col("match_ids.4")).map(_.getString(0)).head)
    assertResult("d2")(df.filter("id=md5('pid2')")
      .select(col("match_ids.4")).map(_.getString(0)).head)
//    assertResult("babf70daa8965dd144ba31e446c00eb7")(df.filter("id='3234567890abcd'")
//      .select(col("match_ids.2")).map(_.getString(0)).head)
//    assertResult("bd2d1deaeda67e4228ef881ee031c314")(df.filter("id='3234567890abcd'")
//      .select(col("match_ids.3")).map(_.getString(0)).head)
  }

//  test("输入为idfa, 输出device mac phone的aes") {
//    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
//    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
//    spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
//         |  uuid='$inputUUID')
//         |select '1234567890abcd' as data
//         |union all
//         |select '2234567890abcd' as data
//         |union all
//         |select '3234567890abcd' as data
//       """.stripMargin)
//    val srcDF = spark.sql(
//      s"""
//         |select '1234567890abcd' idfa, array('d1') device, array('1') device_ltm, 'e1_tm' update_time,
//         |  split('phone1,phone2', ',') phone, split('1,2', ',') phone_ltm,
//         |  split('mac1,mac2', ',') mac, split('1,2', ',') mac_ltm
//         |union all
//         |select '2234567890abcd' idfa, array('d2') device, array('1') device_ltm, 'e1_tm' update_time,
//         |  split('phone1,phone2', ',') phone, split('1,2', ',') phone_ltm,
//         |  split('mac1,mac2', ',') mac, split('1,2', ',') mac_ltm
//         |union all
//         |select '3234567890abcd' idfa, array('d3') device, array('1') device_ltm, 'e1_tm' update_time,
//         |  split('phone1,phone2', ',') phone, split('1,2', ',') phone_ltm,
//         |  split('mac1,mac2', ',') mac, split('1,2', ',') mac_ltm
//       """.stripMargin)
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3_VIEW, Some("day='20190110', plat=1"))
//
//    val input = new IdMappingV3Input(inputType = "uuid", uuid = inputUUID, idType = 7, header = 0)
//    val output = new IdMappingV3Output(uuid = outputUUID, idTypes = Seq(4, 2, 3),
//      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}",
//      encrypt = Encrypt(2, Some(Map("key" -> "1234567890abcdef", "iv" -> "1234567890abcdef"))))
//    val param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = '$outputUUID'
//       """.stripMargin)
//    df.show(false)
//    assertResult("d3")(df.filter("id='3234567890abcd'")
//      .select(col("match_ids.4")).map(_.getString(0)).head)
//    assertResult("VUABWy1CWbKsOnr8DubgFA==")(df.filter("id='3234567890abcd'")
//      .select(col("match_ids.2")).map(_.getString(0)).head)
//  }
//
//  test("输入为phone的aes, 输出device") {
//    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
//    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
//    spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
//         |  uuid='$inputUUID')
//         |select 'O/3ZKPdyw1IKSWqCfqfQdg==,20190305' as data
//         |union all
//         |select 'FB2z3jjiJ0BflUAsujVfmg==,20190305' as data
//       """.stripMargin)
//    val srcDF = spark.sql(
//      s"""
//         |select '18154387820' phone, array('d1') device, array('1') device_ltm, 'e1_tm' update_time
//         |union all
//         |select '18912386146' phone, array('d2') device, array('1') device_ltm, 'e1_tm' update_time
//       """.stripMargin)
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3, Some("day='20190110'"))
//    sql(
//      s"""
//         |drop table if exists ${PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3}_view
//       """.stripMargin)
//    createView(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3, "20190110", "day")
//
//    val input = new IdMappingV3Input(inputType = "uuid", uuid = inputUUID, idType = 3, header = 1,
//      headers = Some(Seq("in_0", "in_1")), sep = Some(","), idx = Some(1),
//      encrypt = Encrypt(2, Some(Map("key" -> "58b17a0f455043a6", "iv" -> "ab703c1d3c68341a"))))
//    val output = new IdMappingV3Output(uuid = outputUUID, idTypes = Seq(4),
//      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(0))
//    val param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = '$outputUUID'
//       """.stripMargin).cache()
//    df.show(false)
//    assertResult("d1")(df.filter("id='O/3ZKPdyw1IKSWqCfqfQdg=='")
//      .select(col("match_ids.4")).map(_.getString(0)).head)
//    assertResult(2)(df.count())
//  }
//
//  test("输入mac，输出imei,mac,phone,device") {
//    val src = spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|2',
//         |  uuid='uuid298')
//         |select 'phone,id,label' as data
//         |union all
//         |select '1,a:b:c,3' as data
//         |union all
//         |select '1,a:b:C,4' as data
//         |union all
//         |select '1,a:B:c,B' as data
//       """.stripMargin)
//
//    val srcDF = spark.sql(
//      s"""
//         |select 'abc' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('1234567890abcde')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('001234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//         |union all
//         |select 'abC' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('01234567890abcd')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('111234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//         |union all
//         |select 'aBc' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('21234567890abcd')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('221234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//       """.stripMargin)
//    srcDF.show(false)
//    println(srcDF.schema)
//    println(spark.table(PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW).schema)
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW, Some("day='20190320'"))
//
//    val input = new IdMappingV3Input(inputType = "uuid", uuid = "uuid298", idType = 2, header = 1,
//      sep = Some(","), idx = Some(2), headers = Some(Seq("in_phone", "in_id", "in_label")))
//    val sss = "/tmp/test_idmapping_V3"
//    val output = new IdMappingV3Output(
//      uuid = "out_uuid",
//      idTypes = Seq(1, 2, 3, 4),
//      hdfsOutput = sss,
//      matchOrigImei = Some(1))
//    val param: IdMappingV3Param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = 'out_uuid'
//       """.stripMargin)
//    df.show(false)
//    assertResult("d2")(df.filter("id='a:b:c'")
//      .select(col("match_ids.4")).map(_.getString(0)).head)
//  }
//  test("输入mac，输出imei,mac,phone,device 加密") {
//    val src = spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|2',
//         |  uuid='uuid298')
//         |select 'phone,id,label' as data
//         |union all
//         |select '1,a:b:c,3' as data
//         |union all
//         |select '1,a:b:C,4' as data
//         |union all
//         |select '1,a:B:c,B' as data
//       """.stripMargin)
//
//    val srcDF = spark.sql(
//      s"""
//         |select 'abc' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('') orig_imei, array('')
//         |orig_imei_tm, array('') orig_imei_ltm,array('001234567890abcde') imei, array('e2_tm') imei_tm, array('')
//         |imei_ltm,array('') imei_md5
//         |union all
//         |select 'abC' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,null orig_imei, null
//         |orig_imei_tm, null orig_imei_ltm,array('111234567890abcde') imei, array('e2_tm') imei_tm, array('e2_ltm')
//         |imei_ltm,array('a1234567890abcde','aaa') imei_md5
//         |union all
//         |select 'aBc' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('21234567890abcd')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('') orig_imei_ltm,array('221234567890abcde') imei, array
//         |('e2_tm') imei_tm, array('e2_ltm') imei_ltm,array('s1234567890abcde') imei_md5
//       """.stripMargin)
//    println(srcDF.schema)
//    println(spark.table(PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW).schema)
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW, Some("day='20190320'"))
//
//    val input = new IdMappingV3Input(inputType = "uuid", uuid = "uuid298", idType = 2, header = 1,
//      sep = Some(","), idx = Some(2), headers = Some(Seq("in_phone", "in_id", "in_label")))
//    val sss = "/tmp/test_idmapping_V3"
//    val output = new IdMappingV3Output(
//      uuid = "out_uuid",
//      idTypes = Seq(1, 2, 3, 4),
//      hdfsOutput = sss,
//      matchOrigImei = Some(1),
//      encrypt = Encrypt(1))
//    val param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = 'out_uuid'
//       """.stripMargin)
//    df.show(false)
//    assertResult("d2")(df.filter("id='a:b:c'")
//      .select(col("match_ids.4")).map(_.getString(0)).head)
//  }
//
//  test("加入一个参数keepSeed:0,输入mac，输出mac，imei,device,phone， 实现keepSeed是否起效果") {
//    val src = spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='0|4',
//         |  uuid='uuid298')
//         |select 'phone,id,label' as data
//         |union all
//         |select '1,a:b:c,3' as data
//         |union all
//         |select '1,a:b:C,4' as data
//         |union all
//         |select '1,a:B:c,B' as data
//       """.stripMargin)
//
//    src.show(false)
//
//    val srcDF = spark.sql(
//      s"""
//         |select 'abc' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('1234567890abcde')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('001234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//         |union all
//         |select 'abC' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('01234567890abcd')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('111234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//         |union all
//         |select 'aBc' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('21234567890abcd')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('221234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//       """.stripMargin)
//    println("srcDF: *************************************")
//    srcDF.show(false)
//
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW, Some("day='20190320'"))
//
//    val input = new IdMappingV3Input(inputType = "uuid", uuid = "uuid298", idType = 2, header = 1,
//      sep = Some(","), idx = Some(2), headers = Some(Seq("in_phone", "in_id", "in_label")))
//    val sss = "/tmp/test_idmapping_V3"
//    val output = new IdMappingV3Output(
//      uuid = "out_uuid",
//      idTypes = Seq(1, 2, 3, 4),
//      hdfsOutput = sss,
//      matchLimit = Some(-1),
//      matchOrigImei = Some(0),
//      keepSeed = Some(0))
//    val param: IdMappingV3Param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = 'out_uuid'
//       """.stripMargin)
//    println("resDF:*************************************")
//    df.show(false)
//
//    val hdfsDF = spark.read.option("header", "false").option("sep", "\t").csv(param.output.hdfsOutput)
//
//    assertResult(2)(hdfsDF.schema.fieldNames.length)
//    assertResult(true)(hdfsDF.schema.fieldNames.contains("idType"))
//    assertResult(8)(hdfsDF.count())
//    assertResult(0)(hdfsDF.filter("idType == 3").count())
//
//  }
//
//  test("检查过滤条件") {
//    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
//    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
//    spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day='20200430', biz='0|4',
//         |  uuid='$inputUUID')
//         |select '1,a:b:c,20200304' as data
//         |union all
//         |select '1,a:b:c,20200305' as data
//         |union all
//         |select '1,a:b:c,20200306' as data
//       """.stripMargin)
//
//    spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
//         |where created_day = '20200430' and biz = '0|4' and uuid = '$inputUUID'
//      """.stripMargin).show(false)
//
//    val srcDF = spark.sql(
//      s"""
//         |select 'abc' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('1234567890abcde')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('001234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//         |union all
//         |select 'abd' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('01234567890abcd')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('111234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//         |union all
//         |select 'aBe' mac, split('d1,d2', ',') device, split('1,2', ',') device_ltm ,array('21234567890abcd')
//         |orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array('221234567890abcde') imei,
//         |array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//       """.stripMargin)
//    println("srcDF: *************************************")
//    srcDF.show(false)
//
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW, Some("day='20200430'"))
//
//    val input = new IdMappingV3Input(inputType = "uuid", uuid = inputUUID, idType = 2, header = 1,
//      sep = Some(","), idx = Some(2), headers = Some(Seq("in_phone", "in_id", "in_day")))
//    val sss = "/tmp/test_idmapping_V3"
//    val output = new IdMappingV3Output(
//      uuid = outputUUID,
//      idTypes = Seq(1, 3, 4),
//      hdfsOutput = sss,
//      matchLimit = Some(-1),
//      matchOrigImei = Some(0),
//      keepSeed = Some(1))
//    val param: IdMappingV3Param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = '$outputUUID'
//       """.stripMargin)
//    println("resDF:*************************************")
//    df.show(false)
//
//
//    assertResult(3)(df.count())
//  }
//
//  test("输入oaid，输出imei,mac,phone,device") {
//    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
//    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
//    spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20200505, biz='3|1',
//         |  uuid='$inputUUID')
//         |select '9fb327046f378d6d6c296d58d4ef9a39fa146b2a3b97aca56dc24e502a37e9c4' as data
//         |union all
//         |select '7bfdeeef-7bfb-2324-fa72-ff2dc7dfd76a' as data
//         |union all
//         |select 'c2c6e8f914c26c07' as data
//       """.stripMargin)
//    val srcDF = spark.sql(
//      s"""
//         |select '9fb327046f378d6d6c296d58d4ef9a39fa146b2a3b97aca56dc24e502a37e9c4' oaid,
//         |  array('8fd168ee04290700e5340ae8efbcad3603ae444a') device, array('1557590400') device_ltm, 'e1_tm'
//         |  update_time,
//         |  split('15899361227,15060302008', ',') phone, split('1535536000,1567664467', ',') phone_ltm,
//         |  split('b8:bc:1b:85:f9:bd,1c:77:f6:30:18:c0', ',') mac, split('1557849600,1557849700', ',') mac_ltm,
//         |  array('01234567890abcd') orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array
//         |  ('111234567890abcde') imei,
//         |  array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//         |union all
//         |select '7bfdeeef-7bfb-2324-fa72-ff2dc7dfd76a' oaid,
//         |  array('8fd16a21c583b6ac6e886f012d941995c731593a') device, array('1557590401') device_ltm, 'e1_tm'
//         |  update_time,
//         |  split('phone1,phone2', ',') phone, split('1,2', ',') phone_ltm,
//         |  split('mac1,mac2', ',') mac, split('1,2', ',') mac_ltm,
//         |  array('1234567890abcde') orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array
//         |  ('001234567890abcde') imei,
//         |  array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//         |union all
//         |select 'c2c6e8f914c26c07' oaid,
//         |  array('8fd1c5aeedabc04c0338efffb3a4159a0c5d14cf') device, array('1557590402') device_ltm, 'e1_tm'
//         |  update_time,
//         |  split('phone1,phone2', ',') phone, split('1,2', ',') phone_ltm,
//         |  split('mac1,mac2', ',') mac, split('1,2', ',') mac_ltm,
//         |  array('21234567890abcd') orig_imei, array('e2_tm') orig_imei_tm, array('e2_ltm') orig_imei_ltm,array
//         |  ('221234567890abcde') imei,
//         |  array('e2_tm') imei_tm, array('e2_ltm') imei_ltm
//       """.stripMargin)
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_OAID_MAPPING_V3_VIEW, Some("day='20200505', plat=1"))
//    srcDF.show(false)
//    val input = new IdMappingV3Input(inputType = "uuid", uuid = inputUUID, idType = 10, header = 0)
//    val output = new IdMappingV3Output(uuid = outputUUID, idTypes = Seq(4, 2, 3, 1),
//      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(0))
//    val param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = '$outputUUID'
//       """.stripMargin)
//    val res1 = df.where($"id" === "9fb327046f378d6d6c296d58d4ef9a39fa146b2a3b97aca56dc24e502a37e9c4")
//      .collectAsList().get(0)
//    val match_id = res1.getMap[Int, String](1)
//    assert(match_id.getOrElse(1, "default").equals("111234567890abcde"))
//    assert(match_id.getOrElse(2, "default").equals("1c:77:f6:30:18:c0"))
//    assert(match_id.getOrElse(3, "default").equals("15060302008"))
//    assert(match_id.getOrElse(4, "default").equals("8fd168ee04290700e5340ae8efbcad3603ae444a"))
//
//    val dataFeature = spark.sql(
//      s"""
//         |select feature
//         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
//         |where uuid = '$outputUUID'
//         |  and feature['seed'][0] = '9fb327046f378d6d6c296d58d4ef9a39fa146b2a3b97aca56dc24e502a37e9c4'
//        """.stripMargin).map(_.getAs[Map[String, Seq[String]]](0)).collect().head
//    assertResult("111234567890abcde")(dataFeature("1").head)
//    assertResult("1c:77:f6:30:18:c0")(dataFeature("2").head)
//    assertResult("15060302008")(dataFeature("3").head)
//    assertResult("8fd168ee04290700e5340ae8efbcad3603ae444a")(dataFeature("4").head)
//  }
//
//  test("当输入数据为data_hub的时候,写出合并了输入的数据") {
//    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
//    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
//    val datahubSourceDF = spark.sql(
//      s"""
//         |select stack(2,
//         |  map('seed', array('m1'), '1_1000', array('20200101', '0')),
//         |  map('seed', array('m2'), '1_1000', array('20200101', '0'))
//         |) as feature
//        """.stripMargin)
//    insertDF2Table(datahubSourceDF, PropUtils.HIVE_TABLE_DATA_HUB, Some(s"uuid='$inputUUID'"))
//
//    val mappingDF = spark.sql(
//      s"""
//         |select stack(1,
//         |  md5('m1'), 'm1', array('p1'), array('20200101')
//         |) as (mac_md5, mac, phone, phone_ltm)
//        """.stripMargin)
//    insertDF2Table(mappingDF, PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3, Some("day='20200101'"))
//    sql(
//      s"""
//         |drop table ${PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3}_view
//       """.stripMargin)
//    createView(PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3, "20200101", "day")
//
//    val input = new IdMappingV3Input(uuid = inputUUID, idType = 2, header = 0, inputType = "dfs")
//    val output = new IdMappingV3Output(uuid = outputUUID, idTypes = Seq(3),
//      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(0))
//    val param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = '$outputUUID'
//       """.stripMargin).cache()
//    val res1 = df.where($"id" === "m1")
//      .collectAsList().get(0)
//    val match_id = res1.getMap[Int, String](1)
//    assert(match_id.getOrElse(3, "default").equals("p1"))
//
//    val dataFeature = spark.sql(
//      s"""
//         |select feature
//         |from ${PropUtils.HIVE_TABLE_DATA_HUB}
//         |where uuid = '$outputUUID'
//         |  and feature['seed'][0] = 'm1'
//        """.stripMargin).map(_.getAs[Map[String, Seq[String]]](0)).collect().head
//    assertResult("p1")(dataFeature("3").head)
//    assertResult("20200101")(dataFeature("3")(1))
//    assertResult(Seq("20200101", "0"))(dataFeature("1_1000"))
//  }
//
//
  test("test pid match device support backtrack dfs") {
    spark.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20200515, biz='3|4',
         |  uuid='uuid777')
         |select 'pid,tm' as data
         |union all
         |select 'pid112233, 20200420' as data
       """.stripMargin)
    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where created_day = '20200515' and biz = '3|4' and uuid = 'uuid777'
      """.stripMargin).show(false)

    val srcDF = spark.sql(
      s"""
         |select 'pid112233' pid
         |     ,  array('1586736000', '1586841153', '1586877153', '1587049953', '1587222753',
         |              '1588432353', '1588475553', '1588734753', '1588986753', '1589235153') as bk_tm
         |     ,  array(0, 4, 2, 3, 1, 3, 2, 4, 0, 3) as device_index
         |     ,  split('1,1,1,1,1', ',') device_plat, split('d1,d2,d3,d4,d5', ',') as device
         |     ,  '20200510' update_time
       """.stripMargin)
    println("srcDF: *************************************")

    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_PID_MAPPING_BK, Some("day='20200515'"))
    createView(PropUtils.HIVE_TABLE_DM_PID_MAPPING_BK, PropUtils.HIVE_TABLE_DM_PID_MAPPING_BK_VIEW)
    spark.table(PropUtils.HIVE_TABLE_DM_PID_MAPPING_BK).show(false)

    val input = new IdMappingV4Input(uuid = "uuid777", idType = 3, encrypt = Encrypt(5), header = 1,
      sep = Some(","), idx = Some(1), headers = Some(Seq("in_pid", "in_tm")), deviceMatch = 2,
      trackDay = Some("20200410"), trackDayIndex = Some(2), inputType = "uuid")
    val sss = "/tmp/test_idmapping_V3"
    val output = new IdMappingV4Output(
      uuid = "out_uuid",
      idTypes = Seq(4),
      hdfsOutput = sss,
      matchLimit = Some(-1),
      matchOrigIeid = Some(0),
      keepSeed = 1)
    val param: IdMappingV4Param = new IdMappingV4Param(Seq(input), output)
    val jobContext = JobContext2(spark, jobCommon, param)
    IdMappingV4.run(jobContext)

    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'out_uuid'
       """.stripMargin)
    println("resDF:*************************************")
    df.show(false)

    assertResult("1586736000")(df.select(element_at($"match_ids", 104)).collect()(0).getAs[String](0))
    assertResult("d1")(df.select(element_at($"match_ids", 4)).collect()(0).getAs[String](0))
  }
//
//  test("test phone match device support backtrack uuid") {
//    spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW} partition(day=20200515, uuid='uuid888')
//         |select 'id1' as id, map(3,'13663714076') as match_ids, 3 as id_type, 4 as encrypt_type
//         |     , 'data000,data111,data222' as data
//       """.stripMargin)
//    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW).show(false)
//
//    val srcDF = spark.sql(
//      s"""
//         |select 'bdff78cdb577c465758c725b6cc60e37' phone_md5, '13663714076' phone
//         |     ,  array('1586736000', '1586841153', '1586877153', '1587049953', '1587222753',
//         |              '1588432353', '1588475553', '1588734753', '1588986753', '1589235153') as bk_tm
//         |     ,  array(0, 4, 2, 3, 1, 3, 2, 4, 0, 3) as device_index
//         |     ,  split('1,1,1,1,1', ',') device_plat, split('d1,d2,d3,d4,d5', ',') as device
//         |     ,  '20200510' update_time
//       """.stripMargin)
//    println("srcDF: *************************************")
//
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK, Some("day='20200515'"))
//    createView(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK_VIEW)
//    spark.table(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK).show(false)
//
//    val input = new IdMappingV3Input(uuid = "uuid888", idType = 3, header = 0,
//      idx = Some(1), deviceMatch = 2,
//      trackDay = Some("20200501"), trackDayIndex = Some(2), inputType = "uuid")
//    val sss = "/tmp/test_idmapping_V3"
//    val output = new IdMappingV3Output(
//      uuid = "out_uuid",
//      idTypes = Seq(4),
//      hdfsOutput = sss,
//      matchLimit = Some(-1),
//      matchOrigImei = Some(0),
//      keepSeed = Some(1))
//    val param: IdMappingV3Param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = 'out_uuid'
//       """.stripMargin)
//    println("resDF:*************************************")
//    df.show(false)
//
//    assertResult("1587222753")(df.select(element_at($"match_ids", 104)).collect()(0).getAs[String](0))
//    assertResult("d2")(df.select(element_at($"match_ids", 4)).collect()(0).getAs[String](0))
//  }
//
//  test("test phone match device support backtrack uuid取种子包中的日期") {
//    spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW} partition(day=20200515, uuid='uuid888')
//         |select 'id1' as id, map(3,'13663714076') as match_ids, 3 as id_type, 4 as encrypt_type
//         |     , 'data000,20200415,data222' as data
//       """.stripMargin)
//    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW).show(false)
//
//    val srcDF = spark.sql(
//      s"""
//         |select 'bdff78cdb577c465758c725b6cc60e37' phone_md5, '13663714076' phone
//         |     ,  array('1586736000', '1586841153', '1586877153', '1587049953', '1587222753',
//         |              '1588432353', '1588475553', '1588734753', '1588986753', '1589235153') as bk_tm
//         |     ,  array(0, 4, 2, 3, 1, 3, 2, 4, 0, 3) as device_index
//         |     ,  split('1,1,1,1,1', ',') device_plat, split('d1,d2,d3,d4,d5', ',') as device
//         |     ,  '20200510' update_time
//       """.stripMargin)
//    println("srcDF: *************************************")
//
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK, Some("day='20200515'"))
//    createView(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK_VIEW)
//    spark.table(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK).show(false)
//
//    val input = new IdMappingV3Input(uuid = "uuid888", idType = 3, header = 0,
//      idx = Some(1), deviceMatch = 2,
//      trackDay = None, trackDayIndex = Some(2), inputType = "uuid")
//    val sss = "/tmp/test_idmapping_V3"
//    val output = new IdMappingV3Output(
//      uuid = "out_uuid",
//      idTypes = Seq(4),
//      hdfsOutput = sss,
//      matchLimit = Some(-1),
//      matchOrigImei = Some(0),
//      keepSeed = Some(1))
//    val param: IdMappingV3Param = new IdMappingV3Param(Seq(input), output)
//    val job = IdMappingV3Job(spark, param, jobCommon)
//    job.submit()
//
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = 'out_uuid'
//       """.stripMargin)
//    println("resDF:*************************************")
//    df.show(false)
//
//    assertResult("1586877153")(df.select(element_at($"match_ids", 104)).collect()(0).getAs[String](0))
//    assertResult("d3")(df.select(element_at($"match_ids", 4)).collect()(0).getAs[String](0))
//  }
//
//  test("test phone match device support backtrack sql") {
//    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
//    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
//    spark.sql(
//      s"""
//         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20200515, biz='3|4',
//         |  uuid='$inputUUID')
//         |select '13663714076,20200420' as data
//       """.stripMargin)
//    spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
//         |where created_day = '20200515' and biz = '3|4' and uuid = '$inputUUID'
//      """.stripMargin).show(false)
//
//    val srcDF = spark.sql(
//      s"""
//         |select 'bdff78cdb577c465758c725b6cc60e37' phone_md5, '13663714076' phone
//         |     ,  array('1586736000', '1586841153', '1586877153', '1587049953', '1587222753',
//         |              '1588432353', '1588475553', '1588734753', '1588986753', '1589235153') as bk_tm
//         |     ,  array(0, 4, 2, 3, 1, 3, 2, 4, 0, 3) as device_index
//         |     ,  split('1,1,1,1,1', ',') device_plat, split('d1,d2,d3,d4,d5', ',') as device
//         |     ,  '20200510' update_time
//       """.stripMargin)
//    println("srcDF: *************************************")
//
//    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK, Some("day='20200515'"))
//    createView(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK_VIEW)
//    spark.table(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_BK).show(false)
//
//    val sql =
//      s"select split(data, @,@)[0] as device, split(data, @,@)[1] as date from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}" +
//        s" where uuid = @$inputUUID@ "
//    val myJson =
//      s"""
//         |{
//         | "jobName": "id_mapping_v3",
//         | "rpcHost": "localhost",
//         | "rpcPort": 0,
//         | "params": [
//         |  {
//         |   "inputs": [
//         |    {
//         |     "idType": 3,
//         |     "deviceMatch": 2,
//         |     "encrypt": {
//         |      "encryptType": 0
//         |     },
//         |     "uuid": "$sql",
//         |     "idx": 1,
//         |     "sep": ",",
//         |     "value": null,
//         |     "header": 1,
//         |     "cleanImei": 0,
//         |     "inputType": "sql",
//         |     "trackDayIndex": 2
//         |    }
//         |   ],
//         |   "output": {
//         |    "idTypes": [
//         |     4
//         |    ],
//         |    "encrypt": {
//         |     "encryptType": 0
//         |    },
//         |    "uuid": "$outputUUID",
//         |    "hdfsOutput": "/tmp/test_idmapping_V3",
//         |    "module": "demo",
//         |    "limit": 2000,
//         |    "keepSeed": 1,
//         |    "matchLimit": 2,
//         |    "matchOrigImei": 0
//         |   }
//         |  }
//         | ],
//         | "jobId": "id_mapping_v3_phone_device_bk_test",
//         | "day": "20190521"
//         |}
//         |""".stripMargin
//
//    IdMappingV3.main(Array(myJson))
//
//    val df = spark.sql(
//      s"""
//         |select *
//         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
//         |where uuid = '$outputUUID'
//       """.stripMargin)
//    println("resDF:*************************************")
//    df.show(false)
//
//    assertResult("1587222753")(df.select(element_at($"match_ids", 104)).collect()(0).getAs[String](0))
//    assertResult("d2")(df.select(element_at($"match_ids", 4)).collect()(0).getAs[String](0))
//  }
//
//  test("test phone => device deviceMatch = 1 保证数据不被过滤") {
//    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
//    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
//    prepareWarehouse()
//    import spark.implicits._
//    // cache创建
//    val dataDF = Seq(
//      "18154387820,20200601", "18154387820,20200605",
//      "18154387821,20200603", "18154387821,20200615",
//      "18154387822,20200512", "18154387823,20200618",
//      "18154387824,20200617", "18154387824,20200618",
//      "18154387825,20200618"
//    ).toDF("data")
//    insertDF2Table(dataDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
//      Some(s"created_day='20200515',biz='3|4',uuid='$inputUUID'"))
//    println("源数据 cache表 =>")
//    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE).where(s"uuid = '$inputUUID'").show(false)
//
//    val phoneDF = spark.sql(
//      s"""
//         |select '18154387820' phone, array('cdf7464d664816d83adb983ff45bc4e6bbc70bf0') device,
//         |       array('1') device_ltm, 'e1_tm' update_time
//         |union all
//         |select '18154387820' phone, array('eeea9b339baf5d7037f8135ce36c57aadca9c661') device,
//         |       array('1') device_ltm, 'e1_tm' update_time
//         |union all
//         |select '18154387821' phone, array('bb18c1a634193ee8c39e9f1c88918f49ec9cbe1c') device,
//         |       array('1') device_ltm, 'e1_tm' update_time
//         |union all
//         |select '18154387822' phone, array('04399d01c2e2fac798f4f1b47285185ebbdca738') device,
//         |       array('1') device_ltm, 'e1_tm' update_time
//         |union all
//         |select '18154387823' phone, array('eeea9b339baf5d7037f8135ce36c57aadca9c661') device,
//         |       array('1') device_ltm, 'e1_tm' update_time
//       """.stripMargin)
//    insertDF2Table(phoneDF, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3, Some("day='20190110'"))
//    createView(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_VIEW)
//
//    val myJson =
//      s"""
//         |{
//         | "jobName": "id_mapping_v3",
//         | "rpcHost": "localhost",
//         | "rpcPort": 0,
//         | "params": [
//         |  {
//         |   "inputs": [
//         |    {
//         |     "idType": 3,
//         |     "deviceMatch": 1,
//         |     "encrypt": {
//         |      "encryptType": 0
//         |     },
//         |     "uuid": "$inputUUID",
//         |     "idx": 1,
//         |     "sep": ",",
//         |     "value": null,
//         |     "header": 1,
//         |     "headers": ["in_phone", "in_date"],
//         |     "cleanImei": 0,
//         |     "inputType": "dfs"
//         |    }
//         |   ],
//         |   "output": {
//         |    "idTypes": [
//         |     4
//         |    ],
//         |    "encrypt": {
//         |     "encryptType": 0
//         |    },
//         |    "uuid": "$outputUUID",
//         |    "hdfsOutput": "/tmp/test_idmapping_V3",
//         |    "module": "demo",
//         |    "limit": 2000,
//         |    "keepSeed": 1,
//         |    "matchLimit": 2,
//         |    "matchOrigImei": 0
//         |   }
//         |  }
//         | ],
//         | "jobId": "id_mapping_v3_phone_device_bk_test",
//         | "day": "20190521"
//         |}
//         |""".stripMargin
//
//    IdMappingV3.main(Array(myJson))
//    println("最终结果cache_new表结果 ==>")
//    val cacheNewDF = spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW).where(s"uuid = '$outputUUID'")
//    val dataHubDF = spark.table(PropUtils.HIVE_TABLE_DATA_HUB).where(s"uuid = '$outputUUID'")
//    cacheNewDF.show(false)
//    dataHubDF.show(false)
//    assertResult(dataDF.count())(cacheNewDF.count)
//    assertResult(dataDF.count())(dataHubDF.count)
//  }

  test("输入为ifid, 输出device 测试唯一性") {
    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|1',
         |  uuid='$inputUUID')
         |select '1234567890ABCD' as data
         |union all
         |select '2234567890ABCD' as data
         |union all
         |select '3234567890ABCD' as data
       """.stripMargin)

    val if1 = Md5Helper.entryMD5_32("1234567890ABCD")
    val if2 = Md5Helper.entryMD5_32("2234567890ABCD")
    val if3 = Md5Helper.entryMD5_32("3234567890ABCD")

    // pid1
    val pid1 = "ab31905ce4745d21acb2d093603073081cdeb41c53473b0d3055327277fc7d02f654ec6b3bef10f5c27ed5e9fb5c37cc"
    // pid2
    val pid2 = "432c516c44aa7806303f0416dfd6ea5d23288f99b7c7881f19e2b35768aa60b9f654ec6b3bef10f5c27ed5e9fb5c37cc"

    val srcDF = sql(
      s"""
         |select '$if1' ifid, array('db', 'da') device, array('1', '2') device_ltm, 'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$if2' ifid, array('d2', 'da', 'dd') device, array('1', '2', '2') device_ltm, 'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$if3' ifid, array('dd', 'da', 'da', 'dc') device, array('1', '3', '3', '2') device_ltm,
         |  'e1_tm' update_time,
         |  array('$pid1', '$pid2') pid, split('1,2', ',') pid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_IFID_MAPPING_VIEW, Some("day='20190110', plat=1"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = inputUUID, idType = 7, header = 0)
    val output = new IdMappingV4Output(uuid = outputUUID, idTypes = Seq(4),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(1))
    val param = new IdMappingV4Param(Seq(input), output)
    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = '$outputUUID'
       """.stripMargin)
    df.show(false)
    assertResult("da")(df.filter(s"id='1234567890ABCD'")
      .select(col("match_ids.4")).map(_.getString(0)).head)
    assertResult("2")(df.filter(s"id='1234567890ABCD'")
      .select(col("match_ids.104")).map(_.getString(0)).head)

    assertResult("dd")(df.filter(s"id='2234567890ABCD'")
      .select(col("match_ids.4")).map(_.getString(0)).head)
    assertResult("2")(df.filter(s"id='2234567890ABCD'")
      .select(col("match_ids.104")).map(_.getString(0)).head)

    assertResult("da")(df.filter(s"id='3234567890ABCD'")
      .select(col("match_ids.4")).map(_.getString(0)).head)
    assertResult("3")(df.filter(s"id='3234567890ABCD'")
      .select(col("match_ids.104")).map(_.getString(0)).head)
  }

  test("输入为pid, 输出device 测试唯一性") {
    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
    val outputUUID = RandomStringUtils.randomAlphanumeric(32)
    val pid1 = "a1605e36bd3ad982ae02c4619a1d4b0bc81d88960a5b269ac57fb5fbfc7beec7f654ec6b3bef10f5c27ed5e9fb5c37cc"
    val pid2 = "306c9f4fd8fbdb35027a36ade7cfe542350cf2eabbd88975b1c3efce162bc7b8f654ec6b3bef10f5c27ed5e9fb5c37cc"
    val pid3 = "a16292253395966ef05029f7332bf1b583abde41c2e1d1f65b69fa95ba177e57f654ec6b3bef10f5c27ed5e9fb5c37cc"
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE} partition(created_day=20190110, biz='3|4',
         |  uuid='$inputUUID')
         |select '$pid1' as data
         |union all
         |select '$pid2' as data
         |union all
         |select '$pid3' as data
       """.stripMargin)

    val srcDF = sql(
      s"""
         |select '$pid1' pid,
         |  array('81aefaae551ed8bc3cc07a7411f9555630ff6ff7', 'a63f85d2c005b9adbca438da48fadc5402431cb7',
         |        '92114fe9c60be953a7c3e9e05f81f68585a299c0', '1f72e066dd5938284ce1c4d19afa215bf0076d7f',
         |        'fc90925a3668ed8cd3ddd42f44ad9e409185e954') device,
         |  array('1525536000', '1525536000', '1525536000', '1525536000', '1525536000') device_ltm,
         |  'e1_tm' update_time,
         |  array('ifid1', 'ifid2') ifid, split('1,2', ',') ifid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$pid2' pid,
         |  array('4e5969ad37720287a205aa1e92b311581f6b8f10', '36bd68b0878afe3332364ce5a0ac4ff6bb5d47bd',
         |        '8d876af0c9a9e1da33d8f673b081e1a59c3972e5') device,
         |  array('1565712000', '1565712000', '1565712000') device_ltm,
         |  'e1_tm' update_time,
         |  array('ifid1', 'ifid2') ifid, split('1,2', ',') ifid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
         |union all
         |select '$pid3' pid,
         |  array('c43c94ac01d4cd7993427134d50c46473843605c', 'fde481b46ff75bab18149d5e9f211708d76cdd22',
         |        '4a5079e6b0db7876c70a3bf31abc099b9422f16c') device,
         |  array('1566921600', '1566921600', '1566921600') device_ltm,
         |  'e1_tm' update_time,
         |  array('ifid1', 'ifid2') ifid, split('1,2', ',') ifid_ltm,
         |  split('mcid1,mcid2', ',') mcid, split('1,2', ',') mcid_ltm
       """.stripMargin)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DM_PID_MAPPING_VIEW, Some("day='20210218'"))

    val input = new IdMappingV4Input(inputType = "uuid", uuid = inputUUID, idType = 3, header = 0,
      encrypt = Encrypt(5), deviceMatch = 0)
    val output = new IdMappingV4Output(uuid = outputUUID, idTypes = Seq(4),
      hdfsOutput = s"tmp/${RandomStringUtils.randomAlphanumeric(32)}", encrypt = Encrypt(0),
      matchLimit = Some(1), keepSeed = 1)
    val param = new IdMappingV4Param(Seq(input), output)
    IdMappingV4.run(JobContext2(spark, jobCommon, param))

    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = '$outputUUID'
       """.stripMargin)
    df.show(false)
    assertResult("fc90925a3668ed8cd3ddd42f44ad9e409185e954")(df.filter(s"id='$pid1'")
      .select(col("match_ids.4")).map(_.getString(0)).head)

    assertResult("8d876af0c9a9e1da33d8f673b081e1a59c3972e5")(df.filter(s"id='$pid2'")
      .select(col("match_ids.4")).map(_.getString(0)).head)

    assertResult("fde481b46ff75bab18149d5e9f211708d76cdd22")(df.filter(s"id='$pid3'")
      .select(col("match_ids.4")).map(_.getString(0)).head)
  }
}
