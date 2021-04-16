package com.mob.dataengine.engine.core.mapping.dataprocessor

import java.io.File

import com.mob.dataengine.commons.JobCommon
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.DataEncryptionDecodingParam
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.{DataFrame, LocalSparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.scalatest.FunSuite


/**
 *
 * @author liyi
 */
class DataEncryptionDecodingTest extends FunSuite with LocalSparkSession{
  import spark.implicits._
  var jobCommon: JobCommon = _
  val PARAMS_KEY = "params"
  var params: Seq[DataEncryptionDecodingParam] = _


  val json = FileUtils.
    getJson("unittest/encryption_decoding/encryption_decoding.json")

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
      .extract[Seq[DataEncryptionDecodingParam]]
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // 创建彩虹表
    spark.sql(sqlText = "DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql(sqlText = "CREATE DATABASE dm_dataengine_mapping")

    val rainImeiSql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/total_imei_md5_mapping.sql",
        tableName = PropUtils.HIVE_TABLE_TOTAL_IMEI_MD5_MAPPING
      )
    spark.sql(rainImeiSql)

    val rainMacSql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/total_mac_md5_mapping.sql",
        tableName = PropUtils.HIVE_TABLE_TOTAL_MAC_MD5_MAPPING
      )
    spark.sql(rainMacSql)

    val rainPhoneSql = FileUtils.
      getSqlScript("conf/sql_scripts/dm_tables_create/dm_dataengine_mapping/total_phone_md5_mapping.sql",
        tableName = PropUtils.HIVE_TABLE_TOTAL_PHONE_MD5_MAPPING
      )
    spark.sql(rainPhoneSql)

    // 准备彩虹表的数据
    prepareRain()

    // 创建存储结果表
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)} CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)}")
    spark.sql(sqlText = s"DROP DATABASE IF EXISTS ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW.split("\\.")(0)} CASCADE")
    spark.sql(sqlText = s"CREATE DATABASE ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW.split("\\.")(0)}")

    val dataOPTCacheSql = FileUtils.
      getSqlScript("conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
        tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE
      )
    println(s"now print single profile info $dataOPTCacheSql")
    createTable(dataOPTCacheSql)

    val dataOPTCacheNewSql = FileUtils.getSqlScript(
      "conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
        tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW)
    println(s"now print single profile info $dataOPTCacheNewSql")
    createTable(dataOPTCacheNewSql)

    // 初始化属性
    init()
  }

  def prepareRain(): Unit = {
    Seq(
      ("13451860417", "d396b5158b1a4faa72257612d04f4d78"),
      ("13817731013", "73d860beb6441f249ec7c830f131eb33"),
      ("13515827720", "63c40df707805e0677db7b063dfe2e5e"),
      ("18523458237", "ead90591504e2999b72c39bf42a2dfb7")
    ).toDF(colNames = "phone", "md5_phone").write.insertInto(tableName = PropUtils.HIVE_TABLE_TOTAL_PHONE_MD5_MAPPING)

    Seq(
      ("b33b7e", "0f2a36a001dfe146ebadede1e6aa0e02"),
      ("b33b7f", "4854fbe25744bc1ef7cd356b53412339"),
      ("b33b7d", "8dcf03262e4cdd038f547ef8d81fab02"),
      ("b33b8e", "5c4ae0b2358f4a802e1a6a34baaa1e06")
    ).toDF(colNames = "mac", "md5_mac").write.insertInto(tableName = PropUtils.HIVE_TABLE_TOTAL_MAC_MD5_MAPPING)

    Seq(
      ("3115015", "a5c3136f3626953d170a74ed2b40d932"),
      ("0d1fd2a", "11afc3ae88ddc85d2f24a34f3e9f471c"),
      ("3115016", "4cf46a6f07f353a5c00598305ffedfed"),
      ("3115017", "7fbcb3ab846afa40732e978e2a2ce03f")
    ).toDF(colNames = "imei", "md5_imei").write.insertInto(tableName = PropUtils.HIVE_TABLE_TOTAL_IMEI_MD5_MAPPING)
  }

  override def afterAll(): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE.split("\\.")(0)} CASCADE")
    spark.sql(s"DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
  }

  def init(): Unit = {
    DataEncryptionDecodingLaunch.main(Array(json))
    prepare(json)
  }

  def getSource(map: Map[String, String], dataProcess: Int, field: String): DataFrame = {
    if (dataProcess == 1) spark.sparkContext.parallelize(map.keys.toSeq, numSlices = 4).toDF(field)
    else spark.sparkContext.parallelize(map.values.toSeq, numSlices = 4).toDF(field)
  }

  def makePhoneMap() : Map[String, String] = {
    Seq(
      ("13451860417", "d396b5158b1a4faa72257612d04f4d78"),
      ("13817731013", "73d860beb6441f249ec7c830f131eb33"),
      ("13515827720", "63c40df707805e0677db7b063dfe2e5e"),
      ("18523458237", "ead90591504e2999b72c39bf42a2dfb7")
    ).toMap[String, String]
  }

  def makeMacMap() : Map[String, String] = {
    Seq(
      ("B0:83:FE:B3:3B:7E", "b083fe,0f2a36a001dfe146ebadede1e6aa0e02"),
      ("B1:83:FE:B3:3B:7F", "b183fe,4854fbe25744bc1ef7cd356b53412339"),
      ("B2:83:FE:B3:3B:7D", "b283fe,8dcf03262e4cdd038f547ef8d81fab02"),
      ("B3:83:FE:B3:3B:8E", "b383fe,5c4ae0b2358f4a802e1a6a34baaa1e06")
    ).toMap[String, String]
  }

  def makeImeiMap() : Map[String, String] = {
    Seq(
      ("864427033115015", "86442703,a5c3136f3626953d170a74ed2b40d932"),
      ("a00000070d1fd2a", "a0000007,11afc3ae88ddc85d2f24a34f3e9f471c"),
      ("864427033115016", "86442703,4cf46a6f07f353a5c00598305ffedfed"),
      ("864427033115017", "86442703,7fbcb3ab846afa40732e978e2a2ce03f")
    ).toMap[String, String]
  }

  /**
   * 从hive中获取结果数据
   * @param tableName 存储结果的表名
   * @param show 是否打印最终结果
   * @return
   */
  def dataFromHive(tableName: String, uuid: String, show: Boolean = false): Array[String] = {
    val resultDF = spark.table(tableName).where(s"uuid = '$uuid'")
    if (show) {
      resultDF.show(truncate = false)
    }
     // 获取脱敏后的数据
     val dataArr = resultDF.select("data").collect().map(_.getAs[String](fieldName = "data"))
     dataArr
   }

  /**
   * 校验hive结果是否准确
   * @param hiveTableName 存储结果的表名
   * @param standardValues
   * @param show 是否打印最终结果
   * @return
   */
  def checkResult(hiveTableName: String, uuid: String,
    standardValues: Seq[String], show: Boolean = false): Unit = {
    val decodingData = dataFromHive(hiveTableName, uuid, show)
    assert(standardValues.intersect(decodingData).length == standardValues.length)
  }

   test(testName = "phone_encrypt" ) {
     val map: Map[String, String] = makePhoneMap()
     val sourceData = getSource(map, dataProcess = 1, field = "phone")

     // 执行业务模块，将phone进行脱敏
     val dataEncryption = DataEncryption(
       spark = spark,
       fieldName = "phone",
       idType = 3,
       outputDay = "20190405",
       outputUuid = "phonedd7fb0ccb4a1d9e23c3eb052fe84e",
       limit = 1000,
       jobName = "data_encrypt_parse",
       hdfsOutputPath = "/tmp/33dd2ac7cb5547a49650e1cb968d0dc3/982dd7fb0ccb4a1d9e23c3eb0xxxphone",
       sourceData = sourceData
    )
     dataEncryption.submit()

     // 核对脱敏的数据和标准的md5是否为能匹配上，如果有一个失败；说明逻辑有问题
     checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE, "phonedd7fb0ccb4a1d9e23c3eb052fe84e",
       map.values.toSeq.map(_.toLowerCase), show = true)
   }

  test(testName = "param_parse_correctly") {
    assert(params.head.output.hdfsOutput == "/tmp/33dd2ac7cb5547a49650e1cb968d0dc3/982dd7fb0ccb4a1d9e23c3eb0md5phone")
  }

  test(testName = "mac_encrypt" ) {
    // 生成mac的数据源
    val map: Map[String, String] = makeMacMap
    val sourceData = getSource(map, dataProcess = 1, field = "mac")

    // 执行业务模块，将mac进行脱敏
    val dataEncryption = DataEncryption(
      spark = spark,
      fieldName = "mac",
      idType = 2,
      outputDay = "20190405",
      outputUuid = "macdd7fb0ccb4a1d9e23c3eb052fe84e",
      limit = 4,
      jobName = "data_encrypt_parse",
      hdfsOutputPath = "/tmp/33dd2ac7cb5547a49650e1cb968d0dc3/982dd7fb0ccb4a1d9e23c3eb052md5mac",
      sourceData = sourceData
    )
    dataEncryption.submit()

    // 核对脱敏的数据和标准的md5是否为能匹配上，如果有一个失败；说明逻辑有问题
    checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE, "macdd7fb0ccb4a1d9e23c3eb052fe84e",
      map.values.toSeq.map(_.toLowerCase), show = true)
  }

  test(testName = "imei_encrypt" ) {
    // 生成imei数据源
    val map: Map[String, String] = makeImeiMap
    val sourceData = getSource(map, dataProcess = 1, field = "imei")

    // 执行业务模块，将imei进行脱敏
    val dataEncryption = DataEncryption(
      spark = spark,
      fieldName = "imei",
      idType = 1,
      outputDay = "20190405",
      outputUuid = "imeidd7fb0ccb4a1d9e23c3eb052fe84e",
      limit = 2,
      jobName = "data_encrypt_parse",
      hdfsOutputPath = "/tmp/33dd2ac7cb5547a49650e1cb968d0dc3/982dd7fb0ccb4a1d9e23c3eb0md5imei",
      sourceData = sourceData
    )
    dataEncryption.submit()

    // 核对脱敏的数据和标准的md5是否为能匹配上，如果有一个失败；说明逻辑有问题
    checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE, uuid = "imeidd7fb0ccb4a1d9e23c3eb052fe84e",
      map.values.toSeq.map(_.toLowerCase), show = true)
  }

   test(testName = "phone_decoding") {
    // 生成数据源phone
    val map: Map[String, String] = makePhoneMap()
    val sourceData = getSource(map, dataProcess = 2, field = "phone")

    // 执行业务模块，将phone逆向为原值
    val dataDecoding = DataDecoding(
      spark = spark,
      fieldName = "phone",
      idType = 3,
      outputDay = "20190403",
      outputUuid = "phone7fb0ccb4a1d9e23c3eb052fe84e",
      limit = 1000,
      hdfsOutputPath = "/tmp/33dd2ac7cb5547a49650e1cb968d0dc3/982dd7fb0ccb4a1d9e23c3ebxxxphone",
      jobName = "data_encrypt_parse",
      sourceData = sourceData
    )
    dataDecoding.submit()

     checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE, uuid = "phone7fb0ccb4a1d9e23c3eb052fe84e",
       map.keys.toSeq.map(_.toLowerCase), show = true)
  }

  test(testName = "mac_decoding") {
    // 生成数据源: Mac
    val map: Map[String, String] = makeMacMap()
    val sourceData = getSource(map, dataProcess = 2, field = "mac")

    // 执行业务模块，将mac逆向为原值
    val dataDecoding = DataDecoding(
      spark = spark,
      fieldName = "mac",
      idType = 2,
      outputDay = "20190403",
      outputUuid = "macdd7fb0ccb4a1d9e23c3eb052fe84e",
      limit = 1000,
      hdfsOutputPath = "/tmp/33dd2ac7cb5547a49650e1cb968d0dc3/982dd7fb0ccb4a1d9e23c3eb0xxxmac",
      jobName = "data_encrypt_parse",
      sourceData = sourceData
    )
    dataDecoding.submit()

    // 将原值与md5逆向值进行交集运算，结果一一对上才说明测试通过
    checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE, uuid = "macdd7fb0ccb4a1d9e23c3eb052fe84e",
      map.keys.toSeq.map(_.toLowerCase), show = true)
  }

  test(testName = "imei_decoding") {
    // 生成数据源: Imei
    val map: Map[String, String] = makeImeiMap()
    val sourceData = getSource(map, dataProcess = 2, field = "imei")

    // 执行业务模块，将imei逆向为原值
    val dataDecoding = DataDecoding(
      spark = spark,
      fieldName = "imei",
      idType = 1,
      outputDay = "20190403",
      outputUuid = "imeidd7fb0ccb4a1d9e23c3eb052fe84e",
      limit = 1000,
      hdfsOutputPath = "/tmp/33dd2ac7cb5547a49650e1cb968d0dc3/982dd7fb0ccb4a1d9e23c3eb05xxximei",
      jobName = "data_encrypt_parse",
      sourceData = sourceData
    )

    dataDecoding.submit()

    // 将原值与md5逆向值进行交集运算，结果一一对上才说明测试通过
    checkResult(PropUtils.HIVE_TABLE_DATA_OPT_CACHE, uuid = "imeidd7fb0ccb4a1d9e23c3eb052fe84e",
      map.keys.toSeq.map(_.toLowerCase), show = true)
  }
}
