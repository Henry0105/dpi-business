package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils
import com.mob.dataengine.utils.idmapping.IdMappingToolsV2.{mergeLists, transDate}
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class IdMappingExternalFullTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
    spark.conf.set("spark.sql.shuffle.partitions", 10)
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database dm_mobdi_mapping")
    val idMappingFullSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/id_mapping_external.sql", PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL)
    createTable(idMappingFullSql)
    val idMappingIncrSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/id_mapping_external.sql", PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC)
    createTable(idMappingIncrSql)
    val idMappingIncrSql1 = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_device_mapping.sql", PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC)
    createTable(idMappingIncrSql1)

    prepareWarehouse()
  }

  def prepareWarehouse(): Unit = {
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict;")
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_mobdi_mapping CASCADE")
  }

  test("id_mapping external full test 初始full表为空") {
    prepareWarehouse()
    val extIncrDF = spark.sql(
      s"""
         |select "8ee3768c96e1f1225acfd2e6aa0e0a7f" owner_data, "18945033656" ext_data,
         | "20190721" processtime, "20190721" day, "imeimd5_phone" type
         |union all
         |select "86845503393213" owner_data, "18100968585" ext_data,
         |  "20190721" processtime, "20190721" day, "imei_phone" type
         |union all
         |select "e63be91c0dbafc82e1c88413f23cd093" owner_data, "13702935144" ext_data,
         | "20190721" processtime, "20190721" day, "imeimd5_phone" type
         |union all
         |select "f84c1c5ceff5a3513dc9959c44c7ebb1" owner_data, "13377171165" ext_data,
         | "20190721" processtime, "20190721" day, "imeimd5_phone" type
         |union all
         |select "35484309219350" owner_data, "18118394583" ext_data,
         |  "20190721" processtime, "20190721" day, "imei_phone" type
         |union all
         |select "13008163565" owner_data, "460014449027437" ext_data,
         |  "20190721" processtime, "20190721" day, "phone_imsi" type
         |union all
         |select "13011681683" owner_data, "460011681089026" ext_data,
         |  "20190721" processtime, "20190721" day, "phone_imsi" type
         |union all
         |select "13011681683" owner_data, "460014449027437" ext_data,
         |  "20190721" processtime, "20190721" day, "phone_imsi" type
       """.stripMargin).toDF("owner_data", "ext_data", "processtime", "day", "type")
    insertDF2Table(extIncrDF, PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC, Some("day,type"), false)

    val dfSrc = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC).cache()

    spark.udf.register("merge_list", mergeLists _)
    spark.udf.register("trans_date", transDate _)

    new IdMappingExternalFull(spark, "20190721", Some(true), false).run()
    val df = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL).cache()

    val extDataArray = df.select("ext_data").where($"owner_data" === "13011681683").take(1)(0).getAs[Seq[String]](0)

    // 测试ltm是否取到以及update_time是否为ltm的值
    assertResult(extDataArray.toSet)(Set("460014449027437", "460011681089026"))
    assertResult(Seq("phone_imsi", "imsi_phone", "imei_phone", "phone_imei"))(
      df.select("type").map(_.getAs[String](0)).distinct().collect().toSeq)
  }

  test("id_mapping external full test 初始full表非空 增量数据日期小于全量表日期") {
    prepareWarehouse()
    val extIncrDF = spark.sql(
      s"""
         |select "6fefcc4d4bf792d4d5581d9805e4a241" owner_data, "18945033657" ext_data,
         | "20190711" processtime, "20190711" day, "imeimd5_phone" type
         |union all
         |select "86845503393213" owner_data, "18100968583" ext_data,
         |  "20190711" processtime, "20190711" day, "imei_phone" type
         |union all
         |select "86845503393214" owner_data, "d9955793d692648d659e5a1c5af1eaae" ext_data,
         |  "20190711" processtime, "20190711" day, "imei_phonemd5" type
         |union all
         |select "86845503393214" owner_data, "18100968586" ext_data,
         |  "20190711" processtime, "20190711" day, "imei_phone" type
         |union all
         |select "86845503393215" owner_data, "18100968587" ext_data,
         |  "20190711" processtime, "20190711" day, "imei14_phone" type
         |union all
         |select "868455033932156" owner_data, "18100968588" ext_data,
         |  "20190711" processtime, "20190711" day, "imei15_phone" type
       """.stripMargin).toDF("owner_data", "ext_data", "processtime", "day", "type")
    insertDF2Table(extIncrDF, PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC, Some("day,type"), false)

    val extFullDF = spark.sql(
      s"""
         |select "86845503393212" owner_data, "d10340d7b50388a516c8c27f80b40d87" owner_data_md5, array("") ext_data,
         | array("fea9aca684e9e5a119b5b04e73d2551f") ext_data_md5,
         | array("1563638400") ext_data_tm, "20190721" day, "imei_phone" type
         |union all
         |select "86845503393213" owner_data, "655639f6adcae93d77f60837a700d3a2" owner_data_md5,
         | array("18100968585") ext_data, array("50069094aa2dc84f6b4b3667579db277") ext_data_md5,
         | array("1563638400") ext_data_tm, "20190721" day, "imei_phone" type
       """.stripMargin).toDF("owner_data", "owner_data_md5", "ext_data", "ext_data_md5",
      "ext_data_tm", "day", "type")
    insertDF2Table(extFullDF, PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL, Some("day,type"), false)


    val dfSrc = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC).cache()

    spark.udf.register("merge_list", mergeLists _)
    spark.udf.register("trans_date", transDate _)

    new IdMappingExternalFull(spark, "20190711", Some(false), false).run()
    val df = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL).cache()

    val extDataArray = df.select("ext_data").where($"owner_data" === "86845503393213").take(1)(0).getAs[Seq[String]](0)

    // 测试ltm是否取到以及update_time是否为ltm的值
    assertResult(extDataArray)(Seq("18100968585", "18100968583"))

    val extDataContainNullArray = df.select("ext_data").where($"owner_data" === "86845503393214")
      .take(1)(0).getAs[Seq[String]](0)

    // 测试部分外部数据只有md5时 明文是否有占位
    assertResult(extDataContainNullArray)(Seq("18100968586", ""))
    
    assertResult(spark.sql(s"show partitions ${PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL}")
      .collect().map(_.getAs[String](0))
      .map(par => par.split("\\/")(1).split("=")(1)
        .replaceAll("md5", "") ).distinct
      .count(tmpType => tmpType.contains("imei1")))(0)
    spark.sql(
      s"""
         |select * from
         |${PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC_VIEW}
         |
       """.stripMargin).show(false)

  }


  test("id_mapping external full test 初始full表非空 增量数据日期大于全量表日期") {
    prepareWarehouse()
    val extIncrDF = spark.sql(
      s"""
         |select "6fefcc4d4bf792d4d5581d9805e4a241" owner_data, "18945033657" ext_data,
         | "20190723" processtime, "20190723" day, "imeimd5_phone" type
         |union all
         |select "86845503393213" owner_data, "18100968583" ext_data,
         |  "20190723" processtime, "20190723" day, "imei_phone" type
         |union all
         |select "868455033932134" owner_data, "18100968582" ext_data,
         |  "20190723" processtime, "20190723" day, "imei_phone" type
         |union all
         |select "123456789012345" owner_data, "18100968582" ext_data,
         |  "20190723" processtime, "20190723" day, "imei15_phone" type
         |union all
         |select "12345678901234" owner_data, "18100968583" ext_data,
         |  "20190723" processtime, "20190723" day, "imei14_phone" type
         |union all
         |select "86845503393213" owner_data, "imsi_1" ext_data,
         |  "20190723" processtime, "20190723" day, "imei_imsi" type
       """.stripMargin).toDF("owner_data", "ext_data", "processtime", "day", "type")
    insertDF2Table(extIncrDF, PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC, Some("day,type"), false)

    val extFullDF = spark.sql(
      s"""
         |select "86845503393211" owner_data, "69cfb857d48b504f4d670632de24c00c" owner_data_md5, array("18945033656") ext_data,
         | array("fea9aca684e9e5a119b5b04e73d2551f") ext_data_md5,
         | array("1563638400") ext_data_tm, "20190721" day, "imei_phone" type
         |union all
         |select "86845503393213" owner_data, "655639f6adcae93d77f60837a700d3a2" owner_data_md5,
         | array("18100968585") ext_data, array("50069094aa2dc84f6b4b3667579db277") ext_data_md5,
         | array("1563638400") ext_data_tm, "20190721" day, "imei_phone" type
         | union all
         |select "86845503393213" owner_data, "655639f6adcae93d77f60837a700d3a2" owner_data_md5,
         | array("18100968585", "18100968582") ext_data,
         |   array("50069094aa2dc84f6b4b3667579db277", md5('18100968582')) ext_data_md5,
         | array("1563638400", '1563638401') ext_data_tm, "20190721" day, "imei_14_phone" type
       """.stripMargin).toDF("owner_data", "owner_data_md5", "ext_data", "ext_data_md5",
      "ext_data_tm", "day", "type")
    insertDF2Table(extFullDF, PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL, Some("day,type"), false)


    val dfSrc = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC).cache()

    spark.udf.register("merge_list", mergeLists _)
    spark.udf.register("trans_date", transDate _)

    val tool = new IdMappingExternalFull(spark, "20190723", Some(false), false)
    tool.prepare()
    tool.run()
    val df = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL).cache()
    df.show(false)

    assertResult(Seq("imsi_1"))(df.where($"owner_data" === "86845503393213" && $"type" === "imei_imsi")
      .select("ext_data").take(1)(0).getAs[Seq[String]](0))

    val extDataArray = df.select("ext_data").where($"day"==="20190723" &&
      $"owner_data" === "86845503393213" && $"type" === "imei_phone").take(1)(0).getAs[Seq[String]](0)

    // 测试ltm是否取到以及update_time是否为ltm的值
    // imei 数据也做成了14位放进去了
    assertResult(Seq("18100968585", "18100968582", "18100968583"))(extDataArray)

    val extData14Array = df.select("ext_data").where($"day"==="20190723" &&
      $"owner_data" === "86845503393213" && $"type" === "imei_14_phone").take(1)(0).getAs[Seq[String]](0)
    assertResult(Seq("18100968585", "18100968582", "18100968583"))(extData14Array)
    val extData14Tm = df.select("ext_data_tm").where($"day"==="20190723" &&
      $"owner_data" === "86845503393213" && $"type" === "imei_14_phone").take(1)(0).getAs[Seq[String]](0)
    assertResult(3)(extData14Tm.length)

    assertResult("imei_14_phone")(sql("select reverse_type('phone_imei_14')")
      .map(_.getString(0)).head())
    assertResult("phone_imei_15")(sql("select reverse_type('imei_15_phone')")
      .map(_.getString(0)).head())
    assertResult("phone_imei_14")(sql("select reverse_type('imei_14_phone')")
      .map(_.getString(0)).head())
    assertResult("mac_phone")(sql("select reverse_type('phone_mac')")
      .map(_.getString(0)).head())

    // imei15/imei14的数据也放入了imei
    val imei14ToImei = df.select("ext_data").where($"day"==="20190723" &&
      $"owner_data" === "12345678901234" && $"type" === "imei_phone").take(1)(0).getAs[Seq[String]](0)
    assertResult(Seq("18100968583", "18100968582"))(imei14ToImei)

    val imei15ToImei = df.select("ext_data").where($"day"==="20190723" &&
      $"owner_data" === "123456789012345" && $"type" === "imei_phone").take(1)(0).getAs[Seq[String]](0)
    assertResult(Seq("18100968582"))(imei15ToImei)
  }

  test("test helper function") {
    val obj = new IdMappingExternalFull(spark, "20190711", Some(false), false)

    assertResult("imei15_phone")(obj.reverseType("phone_imei15"))

    // padmd5 test
    assertResult((null, "own", "ext", obj.md5("ext"), "pt", "imei_phone"))(
      obj.padMd5Field("own", "ext", "pt", "imeimd5_phone"))


    // reverse test
    assertResult((null, "own", "ext", obj.md5("ext"), "pt", "phone_imei15"))(
      obj.reverseFields(("ext", obj.md5("ext"), null, "own", "pt", "imei15_phone")))
  }
}
