package com.mob.dataengine.engine.core.profile.export.bt

import org.apache.spark.sql.{LocalSparkSession, SaveMode, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class SplitTestKlass(override val spark: SparkSession) extends ProfileBatchAbstract with Serializable {
  import spark.implicits._


  override val outputUUID: String = "tmp_uuid"
  override val outputHDFS: Option[String] = Some("tmp/split_test")
  override val outputHDFSLanguage: String = "cn"
  override def start(): Unit = {}
  override def cal(): Unit = {}

  finalDF = Seq(
    // 第1个设备
    ("ff8bb4dfa475d0653ecc31caea8c57eb00df7501", Map("1_1000" -> Seq("", "11"), "2_1000" -> Seq("", "2")),
      "20190501"),
    ("ff8bb4dfa475d0653ecc31caea8c57eb00df7501", Map("1_1000" -> Seq("", "12"), "3_1000" -> Seq("", "3")),
      "20190502"),
    ("ff8bb4dfa475d0653ecc31caea8c57eb00df7501", Map("1_1000" -> Seq("", "13"), "4_1000" -> Seq("", "4")),
      "20190503"),
    // 第2个设备
    ("ff8bb4dfa475d0653ecc31caea8c57eb00df7502", Map("5_1000" -> Seq("", "21"), "2_1000" -> Seq("", "3"),
      "2_1001" -> Seq("", "5")),
      "20190501"),
    // 第3个设备
    ("ff8bb4dfa475d0653ecc31caea8c57eb00df7503", Map("1_1000" -> Seq("", "31")), "20190501"),
    // 第4个设备
    ("ff8bb4dfa475d0653ecc31caea8c57eb00df7504", Map("1_1000" -> Seq("", "31")), "20190502"),
    // 第5个设备
    ("ff8bb4dfa475d0653ecc31caea8c57eb00df7505", Map("9999_1000" -> Seq("", "31")), "20190503")
  ).toDF( "device", "features", "day")
}


class ProfileBatchSplitterTest extends FunSuite with Matchers with LocalSparkSession {
  test("对文件进行切分,可以读到多个文件") {
    val testObj = new SplitTestKlass(spark)
//    spark.emptyDataFrame.write.mode(SaveMode.Overwrite).csv(testObj.outputHDFS.get)
    testObj.inIdsFull.append("1_1000", "2_1000", "2_1001", "3_1000", "4_1000", "5_1000", "9999_1000")
    testObj.export(testObj.inIdsFull, 1)
    val resDF = spark.read
      .option("header", "true")
      .option("quote", "\u0000")
      .csv(testObj.outputHDFS.get + s"/*/*.csv")

    resDF.show(false)

    // 一条记录被切成了2条
    assertResult(testObj.finalDF.count() + 1)(resDF.count())
    deleteHdfsPath(testObj.outputHDFS.get)
  }

  test("不对文件切分,只有1个文件") {
    val testObj = new SplitTestKlass(spark)
//    spark.emptyDataFrame.write.mode(SaveMode.Overwrite).csv(testObj.outputHDFS.get)
    testObj.inIdsFull.append("1_1000", "2_1000", "2_1001", "3_1000", "4_1000", "5_1000", "9999_1000")
    testObj.export(testObj.inIdsFull, 10)
    val resDF = spark.read
      .option("header", "true")
      .option("quote", "\u0000")
      .csv(testObj.outputHDFS.get + "/0/*.csv")
    resDF.show(false)
    assertResult(testObj.finalDF.count())(resDF.count())
    deleteHdfsPath(testObj.outputHDFS.get)
  }
}
