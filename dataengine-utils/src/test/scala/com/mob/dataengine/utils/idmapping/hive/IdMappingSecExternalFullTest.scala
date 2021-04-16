package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils
import com.mob.dataengine.utils.idmapping.IdMappingToolsV2.{mergeLists, transDate}
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class IdMappingSecExternalFullTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
    spark.conf.set("spark.sql.shuffle.partitions", 10)
    sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    sql("DROP DATABASE IF EXISTS mobdi_test CASCADE")
    sql("create database dm_dataengine_test")
    sql("create database mobdi_test")
    val idMappingFullSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_mapping_sec.sql", PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL)
    createTable(idMappingFullSql)
    val idMappingSrcIncrSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_mapping_sec.sql", PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC)
    createTable(idMappingSrcIncrSql)
    val idMappingIncrSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_dataengine_mapping_sec.sql", PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL_INC)
    createTable(idMappingIncrSql)

    prepareWarehouse()
  }

  def prepareWarehouse(): Unit = {
    sql("SET hive.exec.dynamic.partition = true")
    sql("SET hive.exec.dynamic.partition.mode = nonstrict;")
  }

  override def afterAll(): Unit = {
    sql("DROP DATABASE IF EXISTS dm_dataengine_test CASCADE")
    sql("DROP DATABASE IF EXISTS mobdi_test CASCADE")
  }

  test("id_mapping_sec external full test 初始full表为空") {
    prepareWarehouse()
    val extIncrDF = sql(
      s"""
         |select "8ee3768c96e1f1225acfd2e6aa0e0a7f" owner_data, "18945033656" ext_data,
         | "20190721" processtime, "20190721" day, "ieidmd5_pid" type
         |union all
         |select "86845503393213" owner_data, "18100968585" ext_data,
         |  "20190721" processtime, "20190721" day, "ieid_pid" type
         |union all
         |select "e63be91c0dbafc82e1c88413f23cd093" owner_data, "13702935144" ext_data,
         | "20190721" processtime, "20190721" day, "ieidmd5_pid" type
         |union all
         |select "f84c1c5ceff5a3513dc9959c44c7ebb1" owner_data, "13377171165" ext_data,
         | "20190721" processtime, "20190721" day, "ieidmd5_pid" type
         |union all
         |select "35484309219350" owner_data, "18118394583" ext_data,
         |  "20190721" processtime, "20190721" day, "ieid_pid" type
         |union all
         |select "13008163565" owner_data, "460014449027437" ext_data,
         |  "20190721" processtime, "20190721" day, "pid_isid" type
         |union all
         |select "13011681683" owner_data, "460011681089026" ext_data,
         |  "20190721" processtime, "20190721" day, "pid_isid" type
         |union all
         |select "13011681683" owner_data, "460014449027437" ext_data,
         |  "20190721" processtime, "20190721" day, "pid_isid" type
       """.stripMargin).toDF("owner_data", "ext_data", "processtime", "day", "type")
    insertDF2Table(extIncrDF, PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC, Some("day,type"), false)

    val dfSrc = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC).cache()

    spark.udf.register("merge_list", mergeLists _)
    spark.udf.register("trans_date", transDate _)

    val job = new IdMappingSecExternalFull(spark, "20190721", Some(true), false)
    job.prepare()
    job.run()
    val df = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL).cache()

    val extDataArray = df.select("ext_data").where($"owner_data" === "13011681683").take(1)(0).getAs[Seq[String]](0)

    // 测试ltm是否取到以及update_time是否为ltm的值
    assertResult(extDataArray.toSet)(Set("460014449027437", "460011681089026"))
    assertResult(Seq("pid_isid", "isid_pid", "ieid_pid", "pid_ieid").sorted)(
      df.select("type").map(_.getAs[String](0)).distinct().collect().toSeq.sorted)
  }

  test("id_mapping external full test 初始full表非空 增量数据日期小于全量表日期") {
    prepareWarehouse()
    val extIncrDF = sql(
      s"""
         |select "6fefcc4d4bf792d4d5581d9805e4a241" owner_data, "18945033657" ext_data,
         | "20190711" processtime, "20190711" day, "ieidmd5_pid" type
         |union all
         |select "86845503393213" owner_data, "18100968583" ext_data,
         |  "20190711" processtime, "20190711" day, "ieid_pid" type
         |union all
         |select "86845503393214" owner_data, null ext_data,
         |  "20190711" processtime, "20190711" day, "ieid_pidmd5aes" type
         |union all
         |select "86845503393214" owner_data, "18100968586" ext_data,
         |  "20190711" processtime, "20190711" day, "ieid_pid" type
         |union all
         |select "86845503393215" owner_data, "18100968587" ext_data,
         |  "20190711" processtime, "20190711" day, "ieid14_pid" type
         |union all
         |select "868455033932156" owner_data, "18100968588" ext_data,
         |  "20190711" processtime, "20190711" day, "ieid15_pid" type
       """.stripMargin).toDF("owner_data", "ext_data", "processtime", "day", "type")
    insertDF2Table(extIncrDF, PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC, Some("day,type"), false)

    val extFullDF = spark.sql(
      s"""
         |select "86845503393212" owner_data, null ext_data,
         | array("1563638400") ext_data_tm, "20190721" day, "ieid_pid" type
         |union all
         |select "86845503393213" owner_data,
         | array("18100968585") ext_data,
         | array("1563638400") ext_data_tm, "20190721" day, "ieid_pid" type
       """.stripMargin).toDF("owner_data", "ext_data",
      "ext_data_tm", "day", "type")
    insertDF2Table(extFullDF, PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL, Some("day,type"), false)


    val dfSrc = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC).cache()

    spark.udf.register("merge_list", mergeLists _)
    spark.udf.register("trans_date", transDate _)

    val job = new IdMappingSecExternalFull(spark, "20190711", Some(false), false)
    job.prepare()
    job.run()
    val df = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL).cache()

    val extDataArray = df.select("ext_data").where($"owner_data" === "86845503393213").take(1)(0).getAs[Seq[String]](0)

    // 测试ltm是否取到以及update_time是否为ltm的值
    assertResult(extDataArray)(Seq("18100968585", "18100968583"))

    val extDataContainNullArray = df.select("ext_data").where($"owner_data" === "86845503393214")
      .take(1)(0).getAs[Seq[String]](0)

    assertResult(extDataContainNullArray)(Seq("18100968586"))
    
    assertResult(spark.sql(s"show partitions ${PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL}")
      .collect().map(_.getAs[String](0))
      .map(par => par.split("\\/")(1).split("=")(1)
        .replaceAll("md5", "") ).distinct
      .count(tmpType => tmpType.contains("ieid1")))(0)
    spark.sql(
      s"""
         |select * from
         |${PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL_INC_VIEW}
         |
       """.stripMargin).show(false)

  }


  test("id_mapping external full test 初始full表非空 增量数据日期大于全量表日期") {
    prepareWarehouse()
    val extIncrDF = spark.sql(
      s"""
         |select "6fefcc4d4bf792d4d5581d9805e4a241" owner_data, "18945033657" ext_data,
         | "20190723" processtime, "20190723" day, "ieidmd5_pid" type
         |union all
         |select "86845503393213" owner_data, "18100968583" ext_data,
         |  "20190723" processtime, "20190723" day, "ieid_pid" type
         |union all
         |select "868455033932134" owner_data, "18100968582" ext_data,
         |  "20190723" processtime, "20190723" day, "ieid_pid" type
         |union all
         |select "123456789012345" owner_data, "18100968582" ext_data,
         |  "20190723" processtime, "20190723" day, "ieid15_pid" type
         |union all
         |select "12345678901234" owner_data, "18100968583" ext_data,
         |  "20190723" processtime, "20190723" day, "ieid14_pid" type
         |union all
         |select "86845503393213" owner_data, "isid_1" ext_data,
         |  "20190723" processtime, "20190723" day, "ieid_isid" type
       """.stripMargin).toDF("owner_data", "ext_data", "processtime", "day", "type")
    insertDF2Table(extIncrDF, PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC, Some("day,type"), false)

    val extFullDF = spark.sql(
      s"""
         |select "86845503393211" owner_data,  array("18945033656") ext_data,
         | array("1563638400") ext_data_tm, "20190721" day, "ieid_pid" type
         |union all
         |select "86845503393213" owner_data, array("18100968585") ext_data,
         | array("1563638400") ext_data_tm, "20190721" day, "ieid_pid" type
         | union all
         |select "86845503393213" owner_data,
         | array("18100968585", "18100968582") ext_data,
         | array("1563638400", '1563638401') ext_data_tm, "20190721" day, "ieid14_pid" type
       """.stripMargin).toDF("owner_data", "ext_data", "ext_data_tm", "day", "type")
    insertDF2Table(extFullDF, PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL, Some("day,type"), false)


    val dfSrc = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC).cache()

    spark.udf.register("merge_list", mergeLists _)
    spark.udf.register("trans_date", transDate _)

    val tool = new IdMappingSecExternalFull(spark, "20190723", Some(false), false)
    tool.prepare()
    tool.run()
    val df = spark.table(PropUtils.HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL).cache()
    df.show(false)

    assertResult(Seq("isid_1"))(df.where($"owner_data" === "86845503393213" && $"type" === "ieid_isid")
      .select("ext_data").take(1)(0).getAs[Seq[String]](0))

    val extDataArray = df.select("ext_data").where($"day"==="20190723" &&
      $"owner_data" === "86845503393213" && $"type" === "ieid_pid").take(1)(0).getAs[Seq[String]](0)

    // 测试ltm是否取到以及update_time是否为ltm的值
    assertResult(Seq("18100968585", "18100968583"))(extDataArray)

    val extData14Array = df.select("ext_data").where($"day"==="20190723" &&
      $"owner_data" === "86845503393213" && $"type" === "ieid_pid").take(1)(0).getAs[Seq[String]](0)
    assertResult(Seq("18100968585", "18100968583"))(extData14Array)
    val extData14Tm = df.select("ext_data_tm").where($"day"==="20190723" &&
      $"owner_data" === "86845503393213" && $"type" === "ieid_pid").take(1)(0).getAs[Seq[String]](0)
    assertResult(2)(extData14Tm.length)

    assertResult("ieid14_pid")(sql("select reverse_type('pid_ieid14')")
      .map(_.getString(0)).head())
    assertResult("pid_ieid15")(sql("select reverse_type('ieid15_pid')")
      .map(_.getString(0)).head())
    assertResult("pid_ieid14")(sql("select reverse_type('ieid14_pid')")
      .map(_.getString(0)).head())
    assertResult("mcid_pid")(sql("select reverse_type('pid_mcid')")
      .map(_.getString(0)).head())
  }
}
