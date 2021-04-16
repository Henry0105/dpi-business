package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.utils.FileUtils

class IdMapping2V2Test extends MappingTestBase {
  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()

    val dmMacMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_mac_mapping.sql", PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3)
    createTable(dmMacMappingSql)

    val dmPhoneMappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/dm_phone_mapping.sql", PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3)
    createTable(dmPhoneMappingSql)

    val extmappingSql = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/" +
      s"dm_dataengine_mapping/id_mapping_external.sql",
      tableName = PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC_VIEW)
    createTable(extmappingSql)

    new DeviceMappingV2(spark, day).run()
  }

  test("mac mapping") {
    new IdMapping2V2(spark, day, "mac", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_MAC_MAPPING_V3).filter(s"day='$day'").cache()

    assertResult(Seq("a0000099b03850"))(res.filter("mac='2c:5d:34:b6:b4:49'")
      .select("imei").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("a0000099b03850"))(res.filter("mac='2c:5d:34:b6:b4:49'")
      .select("imei_14").map(_.getSeq[String](0)).collect().head)
    assertResult(1)(res.filter("mac='2c:5d:34:b6:b4:49'")
        .where("imei_15 is null").count())

    assertResult(Seq("861599034995935"))(res.filter("mac='1c:77:f6:30:18:c0'")
      .select("imei").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("861599034995936"))(res.filter("mac='1c:77:f6:30:18:c0'")
      .select("orig_imei").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("86159903499593"))(res.filter("mac='1c:77:f6:30:18:c0'")
      .select("imei_14").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("861599034995935"))(res.filter("mac='1c:77:f6:30:18:c0'")
      .select("imei_15").map(_.getSeq[String](0)).collect().head)

    val idfa = res.filter("mac='e4:25:e7:86:8f:83'").select("idfa")
      .map(_.getSeq[String](0)).collect().head
    val idfaTm = res.filter("mac='e4:25:e7:86:8f:83'").select("idfa_tm")
      .map(_.getSeq[String](0)).collect().head
    val idfaLtm = res.filter("mac='e4:25:e7:86:8f:83'").select("idfa_ltm")
      .map(_.getSeq[String](0)).collect().head

    val idfaInfo = idfa.zip(idfaTm).zip(idfaLtm).map{ case ((id, tm), ltm) => id -> (tm, ltm)}.toMap
    assertResult("1531022764")(idfaInfo("039b3271-03a0-4dca-bbed-8ceedad748e2")._1)
    assertResult("1557849600")(idfaInfo("039b3271-03a0-4dca-bbed-8ceedad748e2")._2)
    assertResult("1529881957")(idfaInfo("a95b0c00-bc49-4c09-a093-bb295b3d2a7e")._1)
    assertResult("1557849600")(idfaInfo("a95b0c00-bc49-4c09-a093-bb295b3d2a7e")._2)
  }

  test("phone mapping") {
    val phone = "15899361227"
    val phoneMd5 = "337aef4d7574fda66fb80e825432739e"
    val extDF = spark.sql(
      s"""
         |select stack(6,
         |  '$phone', '$phoneMd5', array('e1', 'e2'), array(md5('e1'), md5('e2')),
         |     array('20190101', '20190102'), 'phone_imei',
         |  '$phone', '$phoneMd5', array('14e1', 'e3'), array(md5('14e1'), md5('e3')),
         |     array('20190101', '20190102'), 'phone_imei_14',
         |  '$phone', '$phoneMd5', array('e1', '15e3'), array(md5('e1'), md5('15e3')),
         |     array('20190101', '20190102'),  'phone_imei_15',
         |  '$phone', '$phoneMd5', array('s1', ''), array(md5('s1'), md5('e1')),
         |     array('20190101', '20190102'), 'phone_imsi',
         |  '$phone', '$phoneMd5', array('m1', ''), array(md5('m1'), md5('m2')),
         |     array('20190101', '20190102'), 'phone_mac',
         |   '12345678901', md5('12345678901'), array('e1'), array(md5('e1')), array('20190101'), 'phone_imei'
         |) as (owner_data, owner_data_md5, ext_data, ext_data_md5, ext_data_tm, type)
       """.stripMargin)
      .toDF()

    insertDF2Table(extDF, PropUtils.HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC_VIEW)

    val fullDF = Seq(
      ("18947962898", "0f60e57ef1196dbb09c1743d9d349ccb", Seq("e4:25:e7:86:8f:83"), Seq("1529883348"),
        Seq("1529883349"), Seq("a95b0c00-bc49-4c09-a093-bb295b3d2a7e"), Seq("1529883348"), Seq("1529883348"),
        Seq("14deb1a889b97870b190725e8a1f2464b078a816"), "1529883348")
    ).toDF("phone", "phone_md5", "mac", "mac_tm", "mac_ltm",
      "idfa", "idfa_tm", "idfa_ltm", "device", "update_time")

    insertDF2Table(fullDF, PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3, Some(s"day=$yesterday"))

    new IdMapping2V2(spark, day, "phone", false).run()
    val res = spark.table(PropUtils.HIVE_TABLE_DM_PHONE_MAPPING_V3).filter(s"day='$day'").cache()

    assertResult(Seq("14e1", "15e3", "868403028648720", "e1", "e2", "e3"))(res
      .filter(s"phone='$phone'")
      .select("imei").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("14e1", "86840302864872", "e3"))(res.filter(s"phone='$phone'")
      .select("imei_14").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("15e3", "868403028648720", "e1"))(res.filter(s"phone='$phone'")
      .select("imei_15").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("868403028648721"))(res.filter(s"phone='$phone'")
      .select("orig_imei").map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("a95b0c00-bc49-4c09-a093-bb295b3d2a7e"))(res.filter("phone='18947962898'")
      .select("idfa").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("14deb1a889b97870b190725e8a1f2464b078a816"))(res.filter("phone='18947962898'")
      .select("device").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("14deb1a889b97870b190725e8a1f2464b078a816"))(res.filter("phone='18947962898'")
      .select("device").map(_.getSeq[String](0)).collect().head)
    assertResult(null)(res.filter("phone='12345678901'")
      .select("device").map(_.getSeq[String](0)).collect().head)
    assertResult(null)(res.filter("phone='12345678901'")
      .select("device_tm").map(_.getSeq[String](0)).collect().head)
    assertResult(null)(res.filter("phone='12345678901'")
      .select("device_ltm").map(_.getSeq[String](0)).collect().head)
  }
}
