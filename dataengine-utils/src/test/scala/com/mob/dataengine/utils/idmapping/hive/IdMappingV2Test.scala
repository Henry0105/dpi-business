package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.commons.utils.{Md5Helper, PropUtils}
import com.mob.dataengine.utils.FileUtils

class IdMappingV2Test extends MappingTestBase {

  import spark.implicits._

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
    val imsiMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_imsi_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IMSI_MAPPING_V3)
    createTable(imsiMappingSql)

    val imeiMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_imei_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3)
    createTable(imeiMappingSql)

    val idfaMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_idfa_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3)
    createTable(idfaMappingSql)

    val serialnoMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_serialno_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_SERIALNO_MAPPING_V3)
    createTable(serialnoMappingSql)
    val extmappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/id_mapping_external.sql",
      tableName = PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC_VIEW)
    createTable(extmappingSql)
    val aoidMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_oaid_mapping.sql",
      tableName = PropUtils.HIVE_TABLE_DM_OAID_MAPPING_V3)
    createTable(aoidMappingSql)

    new DeviceMappingV2(spark, day).run()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
  }

  // android中的老的device为:
  // 8fd1f8876936a3f6af4799f0809edea93330a154: 864862032256248 (imei)
  // 8fd266622c2ac29250aae34724b1b2e1ee9e39cc: 866654034333002 (imei)
  // 8fd26dca57032df747a8ecc49b3dec4721b8b02f: 353765073068166 (imei)
  // ios中的老的device为:
  // 14deb1a889b97870b190725e8a1f2464b078a816

  test("imei mapping同时添加imei/imei14/imei15") {
    // 准备老的全量数据
    val fullDF = Seq(
      ("a0000099b03850", "7f223f53a1f2bb6dbc77d711ec4b8287", Seq("2c:5d:34:b6:b4:49", ""),
        Seq("2a226be7c1207482e9444e11b2861907", "b653666fb0b0e6d7421dd1826dc88da9"),
        Seq("1557590401", "1557590401"), Seq("1557849500", "1557849500"))
    ).toDF("imei", "imei_md5", "mac", "mac_md5", "mac_tm", "mac_ltm")

    insertDF2Table(fullDF, PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3, Some(s"day=$yesterday, plat=1"))

    new IdMappingV2(spark, day, "imei", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3).filter(s"day='$day'").cache()

    assertResult(Seq("", "2c:5d:34:b6:b4:49"))(filterByKey[Seq[String]](res, "imei", "a0000099b03850", "mac"))
    assertResult(Seq("b653666fb0b0e6d7421dd1826dc88da9",
      "2a226be7c1207482e9444e11b2861907"))(filterByKey[Seq[String]]
      (res, "imei", "a0000099b03850", "mac_md5"))
    assertResult(Seq("1557590401", "1557590400"))(filterByKey[Seq[String]]
      (res, "imei", "a0000099b03850", "mac_tm"))
    assertResult(Seq("1557849500", "1557849600"))(filterByKey[Seq[String]]
      (res, "imei", "a0000099b03850", "mac_ltm"))


    assertResult(Seq("b8:bc:1b:85:f9:bd"))(res.filter("imei='866656021262359'")
      .select("mac").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("b8:bc:1b:85:f9:bd"))(res.filter("imei='86665602126235'")
      .select("mac").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("86665602126235"))(res.filter("imei='866656021262359'")
      .select("imei_14").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("866656021262359"))(res.filter("imei='866656021262359'")
      .select("imei_15").map(_.getSeq[String](0)).collect().head)
  }

  test("imei mapping添加外部的imei/imei_14/imei_15") {
    val imei = "e1"
    val imeiMd5 = "cd3dc8b6cffb41e4163dcbd857ca87da"
    val extDF = spark.sql(
      s"""
         |select stack(6,
         |  '$imei', '$imeiMd5', array('p1', 'p2'), array(md5('p1'), md5('p2')),
         |     array('20190101', '20190102'), 'imei_phone',
         |  '$imei', '$imeiMd5', array('s1', 's3'), array(md5('s1'), md5('s3')),
         |     array('20190101', '20190102'), 'imei_imsi',
         |  '$imei', '$imeiMd5', array('s1', 's3'), array(md5('s1'), md5('s3')),
         |     array('20190101', '20190102'),  'imei_14_imsi',
         |  '$imei', '$imeiMd5', array('s1', ''), array(md5('s1'), 'e1_md5'),
         |     array('20190101', '20190102'), 'imei_15_imsi',
         |  '$imei', '$imeiMd5', array('m1', ''), array(md5('m1'), 'm2_md5'),
         |     array('20190101', '20190102'), 'imei_mac',
         |  null, '$imeiMd5', array('m3', ''), array(md5('m3'), 'm2_md5'),
         |     array('20190101', '20190102'), 'imei_mac'
         |) as (owner_data, owner_data_md5, ext_data, ext_data_md5, ext_data_tm, type)
       """.stripMargin)
      .toDF()

    insertDF2Table(extDF, PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC_VIEW)

    val fullDF = Seq(
      ("a0000099b03850", "7f223f53a1f2bb6dbc77d711ec4b8287", Seq("2c:5d:34:b6:b4:49", ""),
        Seq("2a226be7c1207482e9444e11b2861907", "b653666fb0b0e6d7421dd1826dc88da9"),
        Seq("1557590401", "1557590401"), Seq("1557849500", "1557849500"))
    ).toDF("imei", "imei_md5", "mac", "mac_md5", "mac_tm", "mac_ltm")

    insertDF2Table(fullDF, PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3, Some(s"day=$yesterday, plat=1"))

    new IdMappingV2(spark, day, "imei", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_IMEI_MAPPING_V3).filter(s"day='$day'").cache()

    assertResult(Seq("", "2c:5d:34:b6:b4:49"))(filterByKey[Seq[String]](res, "imei", "a0000099b03850", "mac"))
    assertResult(Seq("b653666fb0b0e6d7421dd1826dc88da9",
      "2a226be7c1207482e9444e11b2861907"))(filterByKey[Seq[String]]
      (res, "imei", "a0000099b03850", "mac_md5"))
    assertResult(Seq("1557590401", "1557590400"))(filterByKey[Seq[String]]
      (res, "imei", "a0000099b03850", "mac_tm"))
    assertResult(Seq("1557849500", "1557849600"))(filterByKey[Seq[String]]
      (res, "imei", "a0000099b03850", "mac_ltm"))

    assertResult(Seq("a0000099b03851"))(res.filter("imei='a0000099b03850'")
      .select("orig_imei").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("b8:bc:1b:85:f9:bd"))(res.filter("imei='866656021262359'")
      .select("mac").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("b8:bc:1b:85:f9:bd"))(res.filter("imei='86665602126235'")
      .select("mac").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("86665602126235"))(res.filter("imei='866656021262359'")
      .select("imei_14").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("866656021262359"))(res.filter("imei='866656021262359'")
      .select("imei_15").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("p1", "p2"))(res.filter(s"imei='$imei'")
      .select("phone").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult("1")(res.filter(s"imei='$imei'")
      .select("plat").map(_.getString(0)).collect().head)

    assertResult(Seq("", "m1", "m3"))(res.filter(s"imei_md5='$imeiMd5'")
      .select("mac").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq(Md5Helper.entryMD5_32("m3"), Md5Helper.entryMD5_32("m1"),
      "m2_md5"))(res.filter(s"imei_md5='$imeiMd5'")
      .select("mac_md5").map(_.getSeq[String](0)).collect().head.sorted)
  }

  test("imsi mapping正确处理内部数据") {
    new IdMappingV2(spark, day, "imsi", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_IMSI_MAPPING_V3).filter(s"day='$day'").cache()
    val imsi = "460006201261679"
    val imsiMd5 = "c448adc0586c55fedde4748e91f12bc8"

    assertResult(Seq("866509036132218"))(res.filter(s"imsi='$imsi'")
      .select("imei").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(null)(res
      .filter(s"imsi='$imsi'").select("imei_md5").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1557590400"))(res.filter(s"imsi='$imsi'")
      .select("imei_tm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1557504000"))(res.filter(s"imsi='$imsi'")
      .select("imei_ltm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("86650903613221"))(res.filter(s"imsi='$imsi'")
      .select("imei_14").map(_.getSeq[String](0)).collect().head)
    assertResult(null)(res.filter(s"imsi='$imsi'")
      .select("imei_14_md5").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1557590400"))(res.filter(s"imsi='$imsi'")
      .select("imei_14_tm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1557504000"))(res.filter(s"imsi='$imsi'")
      .select("imei_14_ltm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("866509036132218"))(res.filter(s"imsi='$imsi'")
      .select("imei_15").map(_.getSeq[String](0)).collect().head)
    assertResult(null)(res.filter(s"imsi='$imsi'")
      .select("imei_15_md5").map(_.getSeq[String](0)).collect().head)
  }

  test("imsi mapping 合并了外部数据源") {
    val imsi = "460006201261679"
    val imsiMd5 = "c448adc0586c55fedde4748e91f12bc8"
    val ts1 = "1546272000"  // 20190101
    val ts2 = "1546358400"  // 20190102
    val extDF = spark.sql(
      s"""
         |select stack(7,
         |  '$imsi', '$imsiMd5', array('p1', 'p2'), array(md5('p1'), md5('p2')),
         |   array('$ts1', '$ts2'), 'imsi_phone',
         |  '$imsi', '$imsiMd5', array('e1', 'e3'), array(md5('e1'), md5('e3')),
         |     array('$ts1', '$ts2'), 'imsi_imei',
         |  '$imsi', '$imsiMd5', array('14e1', 'e3'), array(md5('14e1'), md5('e3')),
         |     array('$ts1', '$ts2'),  'imsi_imei_14',
         |  '$imsi', '$imsiMd5', array('15e1', 'e1'), array(md5('15e1'), md5('e1')),
         |     array('$ts1', '$ts2'), 'imsi_imei_15',
         |  '$imsi', '$imsiMd5', array('m1', ''), array(md5('m1'), 'm2_md5'),
         |     array('$ts1', '$ts2'), 'imsi_mac',
         |  's1', 's1_md5', array('p1', 'p2'), array(md5('p1'), md5('p2')),
         |     array('$ts1', '$ts2'), 'imsi_phone',
         |  null, 's2_md5', array('14e1', 'e3'), array(md5('14e1'), md5('e3')),
         |     array('$ts1', '$ts2'),  'imsi_imei_14'
         |) as (owner_data, owner_data_md5, ext_data, ext_data_md5, ext_data_tm, type)
       """.stripMargin)
      .toDF()

    insertDF2Table(extDF, PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC_VIEW)

    new IdMappingV2(spark, day, "imsi", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_IMSI_MAPPING_V3).filter(s"day='$day'").cache()

    res.show(false)

    // imsi中的imei只包含imei字段,没有扩充的IMEI14/IMEI15
    val imei = res.filter(s"imsi_md5='$imsiMd5'").select("imei")
      .map(_.getSeq[String](0)).collect().head
    val imeiMd5 = res.filter(s"imsi_md5='$imsiMd5'").select("imei_md5")
      .map(_.getSeq[String](0)).collect().head
    val imeiTm = res.filter(s"imsi_md5='$imsiMd5'").select("imei_tm")
      .map(_.getSeq[String](0)).collect().head
    val imeiLtm = res.filter(s"imsi_md5='$imsiMd5'").select("imei_ltm")
      .map(_.getSeq[String](0)).collect().head
    val imeiInfo = imeiMd5.zip(imei).zip(imeiTm).zip(imeiLtm)
      .map{ case (((md5, id), tm), ltm) => md5 -> (id, tm, ltm)}.toMap

    assertResult(None)(imeiInfo.get("17203096aeb764d2e5ae30e9161ea52d")) // 86650903613221 md5
    assertResult(("866509036132218", "1557590400", "1557504000"))(imeiInfo("87591792e01d28acd0ad1930e1ce3226"))
    assertResult(("e1", ts1, ts2))(imeiInfo(Md5Helper.entryMD5_32("e1")))  // ltm也是和tm的值一样,因为tm的含义并不是原有的意思了
    assertResult(("e3", ts2, ts2))(imeiInfo(Md5Helper.entryMD5_32("e3")))
    assertResult(("14e1", ts1, ts1))(imeiInfo(Md5Helper.entryMD5_32("14e1")))
    assertResult(("15e1", ts1, ts1))(imeiInfo(Md5Helper.entryMD5_32("15e1")))

    assertResult(Seq("14e1", "86650903613221", "e3"))(res.filter(s"imsi='$imsi'")
      .select("imei_14").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("17203096aeb764d2e5ae30e9161ea52d",
      Md5Helper.entryMD5_32("e3"), Md5Helper.entryMD5_32("14e1")))(res.filter(s"imsi='$imsi'")
      .select("imei_14_md5").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq(ts1, ts2, "1557590400"))(res.filter(s"imsi='$imsi'")
      .select("imei_14_tm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq(ts1, ts2, "1557504000"))(res.filter(s"imsi='$imsi'")
      .select("imei_14_ltm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("15e1", "866509036132218", "e1"))(res.filter(s"imsi='$imsi'")
      .select("imei_15").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("87591792e01d28acd0ad1930e1ce3226", Md5Helper.entryMD5_32("15e1"),
      Md5Helper.entryMD5_32("e1")))(res.filter(s"imsi='$imsi'")
      .select("imei_15_md5").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("14e1", "e3"))(res.filter(s"imsi_md5='s2_md5'")
      .select("imei_14").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("8fd1c5aeedabc04c0338efffb3a4159a0c5d14cf"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("device").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557590400"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("device_tm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557763200"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("device_ltm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("70b0c250"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("serialno").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557676800"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("serialno_ltm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("866509036132219"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("orig_imei").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557590400"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("orig_imei_tm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557504000"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("orig_imei_ltm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("c2c6e8f914c26c07"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("oaid").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1578757203"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("oaid_tm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1583413425"))(res.filter(s"imsi_md5='$imsiMd5'")
      .select("oaid_ltm").map(_.getSeq[String](0)).collect().head.sorted)

//    // 如果是在外部表中join不到的数据就将其md5置为null
    assertResult(1)(res.filter("imsi='460028392984162'")
      .where("phone_md5 is null").count())
    assertResult(Seq("18739265971"))(res.filter(s"imsi='460028392984162'")
      .select("phone").map(_.getSeq[String](0)).collect().head)
  }

  test("idfa test") {
    new IdMappingV2(spark, day, "idfa", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_IDFA_MAPPING_V3).filter(s"day='$day'").cache()
    val idfa = "a95b0c00-bc49-4c09-a093-bb295b3d2a7e"
    assertResult(Seq("e4:25:e7:86:8f:83"))(res.filter(s"idfa='$idfa'")
      .select("mac").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1529881957"))(res.filter(s"idfa='$idfa'")
      .select("mac_tm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1529923766"))(res.filter(s"idfa='$idfa'")
      .select("mac_ltm").map(_.getSeq[String](0)).collect().head.sorted)
  }

  test("serialno test") {
    new IdMappingV2(spark, day, "serialno", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_SERIALNO_MAPPING_V3).filter(s"day='$day'").cache()
    val serialno = "z2x4c15521016978"
    assertResult(Seq("1c:77:f6:30:18:c0", "b8:bc:1b:85:f9:bd"))(res.filter(s"serialno='$serialno'")
      .select("mac").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557590400", "1557590400"))(res.filter(s"serialno='$serialno'")
      .select("mac_tm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557849600", "1557849600"))(res.filter(s"serialno='$serialno'")
      .select("mac_ltm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("861599034995936", "866656021262350"))(res.filter(s"serialno='$serialno'")
      .select("orig_imei").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557590400", "1557590400"))(res.filter(s"serialno='$serialno'")
      .select("orig_imei_tm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("1557504000", "1557676800"))(res.filter(s"serialno='$serialno'")
      .select("orig_imei_ltm").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(
      Seq("9fb327046f378d6d6c296d58d4ef9a39fa146b2a3b97aca56dc24e502a37e9c4", "7bfdeeef-7bfb-2324-fa72-ff2dc7dfd76a")
    )(res.filter(s"serialno='$serialno'")
      .select("oaid").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1573089535", "1584108502"))(res.filter(s"serialno='$serialno'")
      .select("oaid_tm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1586792740", "1584108502"))(res.filter(s"serialno='$serialno'")
      .select("oaid_ltm").map(_.getSeq[String](0)).collect().head)
  }
  test("oaid test") {
    new IdMappingV2(spark, day, "oaid", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_OAID_MAPPING_V3).filter(s"day='$day'").cache()
    val oaid = "7bfdeeef-7bfb-2324-fa72-ff2dc7dfd76a"
    assertResult(Seq("1c:77:f6:30:18:c0"))(res.filter(s"oaid='$oaid'")
      .select("mac").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1557590400"))(res.filter(s"oaid='$oaid'")
      .select("mac_tm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1557849600"))(res.filter(s"oaid='$oaid'")
      .select("mac_ltm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("861599034995936"))(res.filter(s"oaid='$oaid'")
      .select("orig_imei").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1557590400"))(res.filter(s"oaid='$oaid'")
      .select("orig_imei_tm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("1557504000"))(res.filter(s"oaid='$oaid'")
      .select("orig_imei_ltm").map(_.getSeq[String](0)).collect().head)
  }
}
