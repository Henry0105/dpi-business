package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._


class DeviceMappingV2Test extends MappingTestBase {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  // 往前推2天去取增量数据
  // 单元测试的时间都是 <=20190515
  test("id_mapping test") {
    new DeviceMappingV2(spark, day, false, 10).run()
    spark.table(PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3).
      where($"device" === "8fd16a21c583b6ac6e886f012d941995c731593a").show(false)
    spark.table(PropUtils.HIVE_ORIGINAL_ANDROID_ID_MAPPING_V2).
      where($"device" === "8fd16a21c583b6ac6e886f012d941995c731593a").show(false)
    spark.table(PropUtils.HIVE_ORIGINAL_IOS_ID_MAPPING_V2).
      where($"device" === "14deb1a889b97870b190725e8a1f2464b078a816").show(false)
    val res1 = spark.table(PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3).
      where($"device" === "8fd16a21c583b6ac6e886f012d941995c731593a").collectAsList().get(0)
    val row1 = spark.table(PropUtils.HIVE_ORIGINAL_ANDROID_ID_MAPPING_V2).
      where($"device" === "8fd16a21c583b6ac6e886f012d941995c731593a").collectAsList().get(0)
    val imeiLtm = row1.getAs[String]("imei_ltm")
    val macLtm = row1.getAs[String]("mac_ltm")
    val imsiLtm = row1.getAs[String]("imsi_ltm")
    val phoneLtm = row1.getAs[String]("phone_ltm")
    val serialnoLtm = row1.getAs[String]("serialno_ltm")
    val oaidLtm = row1.getAs[String]("oaid_ltm")
    assert(res1.getAs[Seq[String]]("imei_ltm").head.equals(imeiLtm))
    assert(res1.getAs[Seq[String]]("mac_ltm").head.equals(macLtm))
    assert(res1.getAs[Seq[String]]("imsi_ltm").head.equals(imsiLtm))
    assert(res1.getAs[Seq[String]]("phone_ltm").head.equals(phoneLtm))
    assert(res1.getAs[Seq[String]]("serialno_ltm").head.equals(serialnoLtm))
    val updateLtm = Array(imeiLtm, macLtm, imsiLtm, serialnoLtm, phoneLtm, oaidLtm).sortWith(_ > _).head
    assert(res1.getAs[String]("update_time").equals(updateLtm))

    val res2 = spark.table(PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3).
      where($"device" === "14deb1a889b97870b190725e8a1f2464b078a816").collectAsList().get(0)
    val row2 = spark.table(PropUtils.HIVE_ORIGINAL_IOS_ID_MAPPING_V2).
      where($"device" === "14deb1a889b97870b190725e8a1f2464b078a816").collectAsList().get(0)

    val idfaLtm1 = row2.getAs[String]("idfa_ltm")
    val phoneLtm1 = row2.getAs[String]("phone_ltm")
    val macLtm1 = row2.getAs[String]("mac_ltm")
    val updateLtm1 = Array(idfaLtm1, phoneLtm1, macLtm1).sortWith(_ > _).head
    assert(res2.getAs[Seq[String]]("idfa_ltm").head.equals(idfaLtm1))
    assert(res2.getAs[Seq[String]]("phone_ltm").head.equals(phoneLtm1))
    assert(res2.getAs[Seq[String]]("mac_ltm").head.equals(macLtm1))
    println(res2.getAs[String]("update_time"))
    println(updateLtm1)
    assert(res2.getAs[String]("update_time").equals(updateLtm1))
  }

  test("对imei字段进行截取, 会有去掉数据都情况") {
    import spark.implicits._

    val job = new DeviceMappingV2(spark, day, false, 10)

    val df1 = spark.sql(
      s"""
         |select 'k1', array('12345678901234'), array('t1'), array('l1')
         |union all
         |select 'k2', array('123456789012345', '12345678901234'), array('t2', 't1'), array('l2', 'l1')
         |union all
         |select 'k3', array('1234567890123456'), array('t1'), array('l1')
       """.stripMargin)
      .toDF("key", "imei", "imei_tm", "imei_ltm")
      .withColumn("imei_zipped", arrays_zip($"imei", $"imei_tm", $"imei_ltm"))
      .withColumn("imei_struct", expr(job.trimAndExtract("imei_zipped", "imei", 15)))
      .selectExpr("key", "imei_struct")
      .toDF("key", "imei_struct").cache()


    assertResult(1)(df1.where("key='k1' and size(imei_struct.imei) = 0").count())
    assertResult(Seq("123456789012345"))(df1.where("key='k2'")
      .selectExpr("imei_struct.imei").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("t2"))(df1.where("key='k2'")
      .selectExpr("imei_struct.imei_tm").map(_.getSeq[String](0)).collect().head)
    assertResult(Seq("123456789012345"))(df1.where("key='k3'")
      .selectExpr("imei_struct.imei").map(_.getSeq[String](0)).collect().head)
  }

  test("device_mapping") {
    new DeviceMappingV2(spark, day, false, 10).run()
    val d1 = "8fd168ee04290700e5340ae8efbcad3603ae444a"
    val df = spark.table(PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3).filter(s"day=$day").cache()
    df.show(false)
    assertResult("86665602126235")(df.where(s"device='$d1'")
      .select(col("imei_14")(0)).map(_.getString(0)).collect().head)
    assertResult("866656021262359")(df.where(s"device='$d1'")
      .select(col("imei_15")(0)).map(_.getString(0)).collect().head)
    assertResult(Seq("866656021262359"))(df.where(s"device='$d1'")
      .select(col("imei")).map(_.getSeq[String](0)).collect().head.sorted)
    assertResult(Seq("866656021262350"))(df.where(s"device='$d1'")
      .select(col("orig_imei")).map(_.getSeq[String](0)).collect().head.sorted)

    // 增量的数据和全量的数据是一样的, 并且都在全量中
    val incrDF = spark.sql(
      s"""
         |select * from
         |${PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3_INC}
         |where day = '$day'
       """.stripMargin).cache()

    assertResult(incrDF.count())(df.intersect(incrDF).count())
  }

  test("zip with filter") {
    val df1 = spark.sql(
      s"""
         |select array('e1', 'e2') key, array('t1', 't2') tm, array('lt1', 'lt2') ltm
         |union all
         |select array('e1', 'e2') key, array('t1', 't2') tm, array('lt1', 'lt2') ltm
       """.stripMargin)

    val res = df1.withColumn("ziped", arrays_zip($"key", $"tm", $"ltm"))
      .withColumn("filtered_ziped", expr("filter(ziped, tuple -> length(tuple.key)  == 2)"))
        .withColumn("transed", expr("transform(ziped, tuple -> tuple.key)"))

    res.show(false)
    res.selectExpr("transform(filtered_ziped, stru -> stru.key)").explain(true)

    val lettersDataset = Seq(Array("a", "b", "c", "a")).toDF("letters")
  }
  test("inc test") {
    new DeviceMappingV2(spark, day, false, 10).run()
    assert(spark.sql(
      s"""
         |select * from
         |${PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3_INC}
         |where day = '$day'
       """.stripMargin).count() > 0)
  }
  test("oaid test") {
    new DeviceMappingV2(spark, day, false, 10).run()
    val df = spark.sql(
      s"""
         |select *
         |from ${PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3}
         |where day = $day and oaid is not null
       """.stripMargin)
    assert(df.count() == 10)
    val res1 = spark.table(PropUtils.HIVE_TABLE_DM_DEVICE_MAPPING_V3).
      where($"device" === "8fd16a21c583b6ac6e886f012d941995c731593a").collectAsList().get(0)
    val row1 = spark.table(PropUtils.HIVE_ORIGINAL_ANDROID_ID_MAPPING_V2).
      where($"device" === "8fd16a21c583b6ac6e886f012d941995c731593a").collectAsList().get(0)
    val oaid = row1.getAs[String]("oaid")
    val oaidTm = row1.getAs[String]("oaid_tm")
    val oaidLtm = row1.getAs[String]("oaid_ltm")
    assert(res1.getAs[Seq[String]]("oaid").head.equals(oaid))
    assert(res1.getAs[Seq[String]]("oaid_tm").head.equals(oaidTm))
    assert(res1.getAs[Seq[String]]("oaid_ltm").head.equals(oaidLtm))
  }

}
