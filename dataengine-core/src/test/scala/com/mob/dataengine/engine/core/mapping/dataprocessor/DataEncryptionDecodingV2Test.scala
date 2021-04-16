package com.mob.dataengine.engine.core.mapping.dataprocessor

import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.commons.{Encrypt, JobCommon}
import com.mob.dataengine.commons.utils.{AesHelper, PropUtils}
import com.mob.dataengine.engine.core.jobsparam.{DataEncryptionDecodingInputsV2, DataEncryptionDecodingOutputV2, DataEncryptionDecodingParamV2, JobContext2}
import com.mob.dataengine.utils.FileUtils
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.LocalSparkSession
import org.json4s.DefaultFormats
import org.scalatest.FunSuite

class DataEncryptionDecodingV2Test extends FunSuite with LocalSparkSession with UDFCollections {
  implicit val formats: DefaultFormats.type = DefaultFormats
  val json: String = FileUtils.getJson("/unittest/mapping/data_encryption_decoding_v2.json")

  val jobCommon: JobCommon = new JobCommon("jobId", "jobName", "rpcHost", 0, "20191222")

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine_test CASCADE")

    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database dm_dataengine_test")
    spark.sql("create database rp_dataengine")
    spark.sql("create database rp_dataengine_test")

    // 1.创建彩虹表

    val rainImeiSql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/total_imei_md5_mapping.sql",
        tableName = PropUtils.HIVE_TABLE_TOTAL_IMEI_MD5_MAPPING
      )
    createTable(rainImeiSql)

    val rainMacSql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/total_mac_md5_mapping.sql",
        tableName = PropUtils.HIVE_TABLE_TOTAL_MAC_MD5_MAPPING
      )
    createTable(rainMacSql)

    val rainPhoneSql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/total_phone_md5_mapping.sql",
        tableName = PropUtils.HIVE_TABLE_TOTAL_PHONE_MD5_MAPPING
      )
    createTable(rainPhoneSql)

    // 准备彩虹表的数据
    prepareRain()

    // 2.创建mapping表
    val imeiMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_imei_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3)
    createTable(imeiMappingSql)

    val macMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_mac_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3)
    createTable(macMappingSql)

    val phoneMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_phone_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3)
    createTable(phoneMappingSql)

    val deviceMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_device_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3)
    createTable(deviceMappingSql)

    val idfaMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_idfa_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3)
    createTable(idfaMappingSql)

    val imsiMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/" +
      s"dm_tables_create/dm_dataengine_mapping/dm_imsi_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IMSI_MAPPING_V3)
    createTable(imsiMappingSql)

    val codeMappingSql = FileUtils.getSqlScript("conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_code_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING)
    println(s"now print code mapping info $codeMappingSql")
    createTable(codeMappingSql)

    // 准备mapping表
    prepareMapping()

    // 3.创建存储结果表
    val dataHubSql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/profile/single_profile_info.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_HUB)
    println(s"now print data hub info $dataHubSql")
    createTable(dataHubSql)

    val dataOPTCacheNewSql = FileUtils.getSqlScript(s"/conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    createTable(dataOPTCacheNewSql)

    val dataOPTCacheSql = FileUtils.getSqlScript(s"/conf/sql_scripts/rp_tables_create/" +
      s"rp_dataengine/data_opt_cache.sql",
      tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    createTable(dataOPTCacheSql)
  }

  def prepareRain(): Unit = {
    Seq(
      ("13451860417", "d396b5158b1a4faa72257612d04f4d78"),
      ("13817731013", "73d860beb6441f249ec7c830f131eb33"),
      ("13515827720", "63c40df707805e0677db7b063dfe2e5e"),
      ("18523458237", "ead90591504e2999b72c39bf42a2dfb7")
    ).toDF(colNames = "phone", "md5_phone").write.insertInto(tableName = PropUtils.HIVE_TABLE_TOTAL_PHONE_MD5_MAPPING)
  }

  def prepareMapping(): Unit = {
    // 1.imei
    //    insertDF2Table(imeiDF, PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3, Some("day='20191212'"))

    // 2.mac
    val macDF = makeMacSeq().toDF("mac_md5", "mac")
    insertDF2Table(macDF, PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3, Some("day='20191212'"))
    createView(PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3, PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW)
    // 3.phone
    val phoneDF = makePhoneSeq().toDF("phone_md5", "phone")
    insertDF2Table(phoneDF, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3, Some("day='20191212'"))
    createView(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3_VIEW)

    // 4.device
    //    insertDF2Table(deviceDF, PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3, Some("day='20191212'"))

    // 5.idfa
    val idfaDF = makeIdfaSeq().toDF("idfa_md5", "idfa")
    insertDF2Table(idfaDF, PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3, Some("day='20191212', plat='2'"))
    createView(PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3, PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3_VIEW)

    // 6.imsi
    //    insertDF2Table(imsiDF, PropUtils.HIVE_TABLE_DM_IMSI_MAPPING_V3, Some("day='20191212'"))

    // code mapping
    println("codeDF ==>")
    val codeDF = spark.read.option("header", "true").csv("src/test/resources/dm_dataengine_code_mapping.csv")
    insertDF2Table(codeDF, PropUtils.HIVE_TABLE_DATAENGINE_CODE_MAPPING, Some("day='20200502'"))

  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
  }

  def prepareWarehouse(): Unit = {
    val dataOptDF = Seq(
      "phone,name,age",
      "16602113341,jiandd1,22",
      "16602113342,jiandd2,23",
      "16602113343,jiandd3,24",
      "16602113344,jiandd4,25",
      "16602113345,jiandd5,26"
    ).toDF("data")

    insertDF2Table(dataOptDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20190101', biz='3|1', uuid='jiandd_123'"))
  }

  def makeImeiSeq(): Seq[(String, String)] = {
    Seq(
    )
  }

  def makeMacSeq(): Seq[(String, String)] = {
    Seq(
      ("ff706a21aa4188fa2421fc9c225a7097", "2c:8a:72:74:24:e2"),
      ("e4e329f956c1b6b752d6528ee5469666", "ec:89:14:a5:16:e4"),
      ("e2a773bde2f3dd2824cd2b26e2ab0217", "74:60:fa:34:89:fb"),
      ("e4e355d376241053ad4a46a490273c2c", "ec:5a:86:3f:51:e8"),
      ("e4e38e8c6c5f2034561c464af6f9d4f2", "b6:82:6a:d0:30:d1")
    )
  }

  def makePhoneSeq(): Seq[(String, String)] = {
    Seq(
      ("d396b5158b1a4faa72257612d04f4d78", "13451860417"),
      ("73d860beb6441f249ec7c830f131eb33", "13817731013"),
      ("63c40df707805e0677db7b063dfe2e5e", "13515827720"),
      ("ead90591504e2999b72c39bf42a2dfb7", "18523458237")
    )
  }

  def makeIdfaSeq(): Seq[(String, String)] = {
    Seq(
      ("00000b462bdc49609624c82a708e535d", "7e785789-c5b1-4a87-96b3-5975f42ba597"),
      ("000038735d63cf86be2d14dbf38377f7", "f9cd8e12-8e5a-4b35-9742-f727b7d150f5"),
      ("00018c283b23acae6c6629719bd62789", "2bc05002-d684-4244-8fbc-e0c44ffb1a7e"),
      ("00025ed16ee13b78787672fdfa302442", "1198f566-1166-4843-8a31-a728c50ad6dc"),
      ("000352bc430a22c40f13308f16b85598", "3fb4ddf9-92f6-45b1-b736-85ed8de77c26"),
      ("00059e7cfc4296a0b95cac5b43b0d9e2", "5d72890b-584f-47f6-b9e5-101f27ba5674"),
      ("0006c26762b5060cbcd8556cea456b9e", "fb44155f-dadf-4741-bb0b-693dc1bb8ef2"),
      ("00104873102a6b7388ce75f85055841c", "0cd75698-a818-41c5-9889-3013346a872b")
    )
  }

  /**
   * 从hive中获取结果数据
   *
   * @param tableName 存储结果的表名
   * @param show      是否打印最终结果
   * @return 脱敏后的数据
   */
  def dataFromHive(tableName: String, uuid: String, show: Boolean = false): Array[String] = {
    val resultDF = spark.table(tableName).where(s"uuid = '$uuid'")
    if (show) {
      resultDF.show(truncate = false)
    }
    // 获取脱敏后的数据
    val dataArr = resultDF.where(resultDF("match_ids").isNotNull).select("match_ids").collect().map(_.getMap[String,
      String](0).values.head)
    dataArr
  }

  /**
   * 校验hive结果是否准确
   *
   * @param hiveTableName  存储结果的表名
   * @param standardValues 标准值
   * @param show           是否打印最终结果
   * @return 结果
   */
  def checkResult(hiveTableName: String, uuid: String, standardValues: Seq[String], show: Boolean = false): Unit = {
    val decodingData = dataFromHive(hiveTableName, uuid, show)
    assert(standardValues.intersect(decodingData).length == standardValues.length)
  }

  test("test_01") {
    prepareWarehouse()
    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE}
         |where uuid = 'jiandd_123'
        """.stripMargin).show(false)
    DataEncryptionDecodingLaunchV2.main(Array(json))
    spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |where uuid = 'jiandd_234'
        """.stripMargin).show(false)
  }

  test("test_02 MDML1059 输入mac 解密 aes") {
    // 1.准备数据
    val args = Some(Map[String, String](
      "key" -> "v4lRvcJJV1zW5Tcs",
      "iv" -> "1236966788039762"
    ))
    val srcDF = Seq(
      "mac,name,age",
      "2c:8a:72:74:24:e2,jiandd1,22",
      "ec:89:14:a5:16:e4,jiandd2,23",
      "74:60:fa:34:89:fb,jiandd3,24",
      "ec:5a:86:3f:51:e8,jiandd4,25",
      "b6:82:6a:d0:30:d1,jiandd5,26"
    ).map(line => {
      if (line.contains("mac")) {
        line
      } else {
        val fields = line.split(",")
        val aes = AesHelper.encodeAes(args.get("key"), args.get("iv"), fields.head)
        line.replaceAll(fields.head, aes)
      }
    }).toDF("data")

    srcDF.show(false)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20191212', biz='data_encrypt_parse_v2|2', uuid='MDML1059_mac_aes_input'"))

    // 2.准备参数
    val input = new DataEncryptionDecodingInputsV2(
      uuid = "MDML1059_mac_aes_input", idType = 2, sep = Some(","), idx = Some(1),
      header = 1, headers = Some(Seq("in_mac", "in_name", "in_age")), inputType = "dfs"
    )
    val ouput = new DataEncryptionDecodingOutputV2(
      uuid = "MDML1059_mac_aes_output", hdfsOutput = "", limit = Some(1000), encryption = 1,
      encrypt = Encrypt(2, args)
    )
    val param = new DataEncryptionDecodingParamV2(Seq(input), ouput)

    val ctx = JobContext2(spark, jobCommon, param)
    // 3.执行任务
    DataEncryptionDecodingLaunchV2.run(ctx)

    // 4.断言
    val standardValues = Seq(
      "2c:8a:72:74:24:e2",
      "ec:89:14:a5:16:e4",
      "74:60:fa:34:89:fb",
      "ec:5a:86:3f:51:e8",
      "b6:82:6a:d0:30:d1"
    )
    checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, "MDML1059_mac_aes_output", standardValues, show = true)
  }

  test("test_03 MDML1059 测试增加transform参数之后的效果 输入phone 解密 md5_32") {
    // 1.准备数据
    val srcDF = Seq(
      "phone,name,age",
      "d396b5158b1a4faa72257612d04f4d78,nameA,22",
      "73d860beb6441f249ec7c830f131eb33,nameB,23",
      "63c40df707805e0677db7b063dfe2e5e,nameC,24",
      "ead90591504e2999b72c39bf42a2dfb7,nameD,25"
    ).toDF("data")
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20191212', biz='data_encrypt_parse_v2|3', uuid='MDML1059_phone_md5_32_input'"))

    spark.table(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3).show(false)
    // 2.准备参数
    val input = new DataEncryptionDecodingInputsV2(
      uuid = "MDML1059_phone_md5_32_input", idType = 3, sep = Some(","), idx = Some(1),
      header = 1, headers = Some(Seq("in_phone", "in_name", "in_age")), inputType = "dfs"
    )
    val ouput = new DataEncryptionDecodingOutputV2(
      uuid = "MDML1059_phone_md5_32_output", hdfsOutput = "", limit = Some(1000), encryption = 1, encrypt = Encrypt(1)
    )
    val param = new DataEncryptionDecodingParamV2(Seq(input), ouput)

    // 3.执行任务
    val ctx = JobContext2(spark, jobCommon, param)
    DataEncryptionDecodingLaunchV2.run(ctx)

    // 4.断言
    val standardValues = makePhoneSeq().map(_._2)
    checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, "MDML1059_phone_md5_32_output", standardValues, show = true)
  }

  test("test_04 MDML1059 测试增加transform参数之后的效果 输入mac 解密 md5_32") {
    // 1.准备数据
    val srcDF = Seq(
      "name,mac,age",
      "nameA,7d1b59984ff362c547a61e821f056796,22", // 大写带冒号
      "nameB,42dca92c5e4e18d9ab5222040a923232,23", // 大写不带冒号
      "nameC,e4e355d376241053ad4a46a490273c2c,24", // 小写带冒号
      "nameD,70f4d6aad83964731130a3a5dbc2c015,25", // 小写不带冒号
      "nameE,5c4ae0b2358f4a802e1a6a34baaa1e06,26" // 不存在
    ).toDF("data")
    srcDF.show(false)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20191212', biz='data_encrypt_parse_v2|2', uuid='MDML1059_mac_md5_32_input'"))

    spark.table(PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3).show(false)
    // 2.准备参数
    val transMap01 = Some(Map("lower" -> Seq("0"))) //大写带冒号  "2C:8A:72:74:24:E2"
    val transMap02 = Some(Map("lower" -> Seq("0"), "delete" -> Seq(":"))) //大写不带冒号 "EC8914A516E4"
    val transMap03 = Some(Map("lower" -> Seq("1"))) //小写带冒号   "74:60:fa:34:89:fb"
    val transMap04 = Some(Map("lower" -> Seq("1"), "delete" -> Seq(":"))) //小写不带冒号 "ec5a863f51e8"

    val input = new DataEncryptionDecodingInputsV2(
      uuid = "MDML1059_mac_md5_32_input", idType = 2, sep = Some(","), idx = Some(2),
      header = 1, headers = Some(Seq("in_name", "in_mac", "in_age")), inputType = "dfs"
    )
    val ouput = new DataEncryptionDecodingOutputV2(
      uuid = "MDML1059_mac_md5_32_output", hdfsOutput = "", limit = Some(1000), encryption = 1,
      encrypt = Encrypt(1), transform = transMap02
    )
    val param = new DataEncryptionDecodingParamV2(Seq(input), ouput)

    // 3.执行任务
    val ctx = JobContext2(spark, jobCommon, param)
    DataEncryptionDecodingLaunchV2.run(ctx)

    // 4.断言
    assertResult(1)(dataFromHive(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, "MDML1059_mac_md5_32_output", show = true)
      .length)
  }

  test("test_05 MDML1059 测试增加transform参数之后的效果 输入idfa sha256 没有- ") {
    // 1.准备数据
    val srcDF = Seq(
      "name,idfa,age",
      "nameA,83d89eeeb0d7881d84291f86e1d069afe02b38534cbbb6e239fff6a3a689570b,22",
      "nameB,b3a54ffccb2ab3c4498238ca53e9d813d40d4f5262e4298adbad9e199a1c250b,23",
      "nameC,c3aa7c028961ffb971026e4feba4a085d9c147dfcdf3bc427b61777b7b139a67,24",
      "nameD,7f15864cc2c5361dd55ec2bbff314ac01ab5b294063142438e8fc0e6e07ceb7e,25",
      "nameE,4d7ff0143d312adasjn22akdfca2edkjdbwqbd281u2e91ehdhwddassdas93c4b,26"
    ).toDF("data")
    srcDF.show(false)
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20191212', biz='data_encrypt_parse_v2|7', uuid='MDML1059_idfa_md5_32_input'"))

    // 2.准备参数
    val trans = Some(Map("delete" -> Seq("-")))
    val input = new DataEncryptionDecodingInputsV2(
      uuid = "MDML1059_idfa_md5_32_input", idType = 7, sep = Some(","), idx = Some(2),
      header = 1, headers = Some(Seq("in_name", "in_idfa", "in_age")), inputType = "dfs"
    )
    val ouput = new DataEncryptionDecodingOutputV2(
      uuid = "MDML1059_idfa_md5_32_output", hdfsOutput = "", limit = Some(1000), encryption = 1,
      encrypt = Encrypt(4), transform = trans
    )
    val param = new DataEncryptionDecodingParamV2(Seq(input), ouput)

    // 3.执行任务
    val ctx = JobContext2(spark, jobCommon, param)
    DataEncryptionDecodingLaunchV2.run(ctx)

    // 4.断言
    assertResult(4)(dataFromHive(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, "MDML1059_idfa_md5_32_output", show = true)
      .length)
  }

  test("test_06 MDML1059 测试keepSeed参数 keepSeed是否包含种子包") {
    // 1.准备数据
    val srcDF = Seq(
      "d396b5158b1a4faa72257612d04f4d78",
      "73d860beb6441f249ec7c830f131eb33",
      "63c40df707805e0677db7b063dfe2e5e",
      "ead90591504e2999b72c39bf42a2dfb7",
    ).toDF("data")
    insertDF2Table(srcDF, PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      Some("created_day='20191212', biz='data_encrypt_parse_v2|3', uuid='MDML1059_phone_md5_32_input'"))

    // 2.准备参数
    val trans =
      Some(
        Map(
          "fillImei" -> Seq(),
          "lower" -> Seq(),
          "delete" -> Seq(null)
        ))
    val input = new DataEncryptionDecodingInputsV2(
      uuid = "MDML1059_phone_md5_32_input", idType = 3,
      header = 0, headers = Some(Seq("in_phone")), inputType = "dfs"
    )
    val output = new DataEncryptionDecodingOutputV2(
      uuid = "MDML1059_phone_md5_32_output", hdfsOutput = "/tmp/DataEncryptionDecodingLaunchV2", limit = Some(1000),
      encryption = 1, encrypt = Encrypt(1)
      , keepSeed = 1, transform = trans
    )
    val param = new DataEncryptionDecodingParamV2(Seq(input), output)

    // 3.执行任务
    val ctx = JobContext2(spark, jobCommon, param)
    DataEncryptionDecodingLaunchV2.run(ctx)

    // 4.断言
    val standardValues = makePhoneSeq().map(_._2)
    checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, "MDML1059_phone_md5_32_output", standardValues, show = true)

    val hdfsDF = spark.read.option("header", "false").option("sep", "\t").csv(output.hdfsOutput)
    hdfsDF.show(false)
    assertResult(2)(hdfsDF.columns.length)
  }

  test("当输入数据为data_hub的时候,写出合并了输入的数据") {
    // 1.准备数据
    val inputUUID = RandomStringUtils.randomAlphanumeric(32)
    val outputUUID = RandomStringUtils.randomAlphanumeric(32)

    val datahubSourceDF = spark.sql(
      s"""
         |select stack(5,
         |  map('seed', array('nameA,83d89eeeb0d7881d84291f86e1d069afe02b38534cbbb6e239fff6a3a689570b,22')
         |    , '1_1000', array('20200101', '0')),
         |  map('seed', array('nameB,b3a54ffccb2ab3c4498238ca53e9d813d40d4f5262e4298adbad9e199a1c250b,23')
         |    , '1_1000', array('20200101', '0')),
         |  map('seed', array('nameC,c3aa7c028961ffb971026e4feba4a085d9c147dfcdf3bc427b61777b7b139a67,24')
         |    , '1_1000', array('20200101', '0')),
         |  map('seed', array('nameD,7f15864cc2c5361dd55ec2bbff314ac01ab5b294063142438e8fc0e6e07ceb7e,25')
         |    , '1_1000', array('20200101', '0')),
         |  map('seed', array('nameE,4d7ff0143d312adasjn22akdfca2edkjdbwqbd281u2e91ehdhwddassdas93c4b,26')
         |    , '1_1000', array('20200101', '0'))
         |) as feature
        """.stripMargin)
    insertDF2Table(datahubSourceDF, PropUtils.HIVE_TABLE_DATA_HUB, Some(s"uuid='$inputUUID'"))
    spark.table(PropUtils.HIVE_TABLE_DATA_HUB).show(false)

    // 2.准备参数
    val trans = Some(Map("delete" -> Seq("-")))
    val input = new DataEncryptionDecodingInputsV2(
      uuid = inputUUID, idType = 7, sep = Some(","), idx = Some(2),
      header = 1, headers = Some(Seq("in_name", "in_idfa", "in_age")), inputType = "dfs"
    )
    val ouput = new DataEncryptionDecodingOutputV2(
      uuid = outputUUID, hdfsOutput = "", limit = Some(1000), encryption = 1,
      encrypt = Encrypt(4), transform = trans
    )
    val param = new DataEncryptionDecodingParamV2(Seq(input), ouput)

    // 3.执行任务
    val ctx = JobContext2[DataEncryptionDecodingParamV2](spark, jobCommon, param)
    DataEncryptionDecodingLaunchV2.run(ctx)

    // cache_new 判断
    println("show cache_new ==>")
    assertResult(4)(dataFromHive(PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW, outputUUID, show = true)
      .length)

    // data_hub 判断
    println("show data_hub ==>")
    val dataHubDF = spark.table(PropUtils.HIVE_TABLE_DATA_HUB).where(s"uuid = '$outputUUID'")
    dataHubDF.show(false)

  }

}
