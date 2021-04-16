package com.mob.dataengine.utils

import com.mob.dataengine.utils.idmapping.hive.AggregateIdTypeV2
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class AggregateIdTypeV2Test extends FunSuite {
  test("udaf work") {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.udf.register("aggregateDevice", new AggregateIdTypeV2(200))

    spark.sql(
      """
        |select 'k' key, 'a,b,a' device, '20190204,20190301,20190105' device_tm, '20190105,20190304,20190107' device_ltm
        |union all
        |select 'k' key, 'a,b' device, '20190102,20190109' device_tm, '20190106,20190401' device_ltm
        |union all
        |select 'k' key, 'a' device, '20190103' device_tm, '20190103' device_ltm
      """.stripMargin).createOrReplaceTempView("t1")

    spark.table("t1").show(false)

    val df = spark.sql("select key, aggregateDevice(device, device_tm, device_ltm) agg from t1 group by key")
      .select("key", "agg.device", "agg.device_tm", "agg.device_ltm", "agg.update_time").cache()

    val devices = df.select("device").collect()(0).getAs[String](0).split(",")
    val deviceTms = df.select("device_tm").collect()(0).getAs[String](0).split(",")
    val deviceLtms = df.select("device_ltm").collect()(0).getAs[String](0).split(",")
    val updateTime = df.select("update_time").collect()(0).getAs[String](0)

    val deviceInfo = devices.zip(deviceTms).zip(deviceLtms)
      .map{ case ((device, tm), ltm) => device -> (tm, ltm)}.toMap

    assertResult("20190102")(deviceInfo("a")._1)
    assertResult("20190107")(deviceInfo("a")._2)

    assertResult("20190109")(deviceInfo("b")._1)
    assertResult("20190401")(deviceInfo("b")._2)

    assertResult("20190401")(updateTime)
  }

  test("udaf limit work") {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val limit = 3
    spark.udf.register("aggregateDevice", new AggregateIdTypeV2(limit))

    spark.sql(
      """
        |select 'k' key, 'a,b' device, '20190104,20190101' device_tm, '20190104,20190101' device_ltm
        |union all
        |select 'k' key, 'd,b' device, '20190102,20190301' device_tm, '20190402,20190301' device_ltm
        |union all
        |select 'k' key, 'a,c,b' device, '20190103,20190301,20190201' device_tm, '20190103,20190301,20190201' device_ltm
        |union all
        |select 'k' key, 'c,b' device, '20190201,20190301' device_tm, '20190201,20190301' device_ltm
      """.stripMargin).createOrReplaceTempView("t1")

    spark.table("t1").show(false)

    val res = spark.sql(
      """
        |select key, split(agg.device, ',') device, split(agg.device_tm, ',') device_tm,
        |split(agg.device_ltm, ',') device_ltm, agg.update_time
        |from (
        |  select key, aggregateDevice(device, device_tm, device_ltm) agg
        |  from t1
        |  group by key
        |) as tmp
      """.stripMargin)
    res.show(false)
    assert(res.select("device").take(1)(0).getAs[Seq[_]](0).size == limit )
  }
}
