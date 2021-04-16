package com.mob.dataengine.utils.tags

import com.mob.dataengine.commons.Encrypt
import com.mob.dataengine.commons.utils.PropUtils._
import com.mob.dataengine.utils.FileUtils
import com.mob.dataengine.utils.tags.profile.ExtLableRelation.{MD5ID, NONID, SHA256ID}
import com.mob.dataengine.utils.tags.profile.{ExtLableRelation, Params}
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class ExtLableRelationTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql("DROP DATABASE IF EXISTS dw_ext_exchange CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_tags CASCADE")
    spark.sql("create database dw_ext_exchange")
    spark.sql("create database dm_dataengine_tags")

    val dataSql = FileUtils.getSqlScript(s"conf/sql_scripts/ext_table_create/" +
      s"label_relation.sql", tableName = HIVE_TABLE_LABEL_RELATION)
    createTable(dataSql)

    val dataSql2 = FileUtils.getSqlScript(s"conf/sql_scripts/dm_tables_create/dm_dataengine_tags/" +
      s"ext_label_relation_full.sql", tableName = HIVE_TABLE_EXT_LABEL_RELATION_FULL)
    createTable(dataSql2)

    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("DROP DATABASE IF EXISTS dm_sdk_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_mobdi_app CASCADE")
  }

  def prepareWarehouse(): Unit = {
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val schema = spark.table(HIVE_TABLE_LABEL_RELATION).schema.fieldNames
    // 1:non+md5+sha256 2:non+md5 3:non+sha256 4:non 5:md5 6:sha256
    val df1 = Seq(
      ("13325789091", "phone",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2")),
        "个推", "20200502"
      ),
      ("13325789092", "phone",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2")),
        "个推", "20200502"
      ),
      ("13325789093", "phone",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2")),
        "个推", "20200502"
      ),
      ("13325789094", "phone",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2")),
        "个推", "20200502"
      ),
      ("13325789097", "phone",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2"),
          Map("channelId" -> "APP_HOBY_MIDDLE_BANK", "channelValue" -> "1.0")),
        "个推", "20200502"
      ),
      ("13325789099", "phone",
        Array(Map("channelId" -> "APP_HOBY_MIDDLE_BANK", "channelvalue" -> "1.0")),
        "个推", "20200502"
      ),
      (Encrypt(MD5ID).compute("13325789091"), "phonemd5",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D007", "mobvalue" -> "3")),
        "个推", "20200502"
      ),
      (Encrypt(MD5ID).compute("13325789092"), "phonemd5",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2")),
        "个推", "20200502"
      ),
      (Encrypt(MD5ID).compute("13325789095"), "phonemd5",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2")),
        "个推", "20200502"
      ),
      (Encrypt(SHA256ID).compute("13325789091"), "phonesha256",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D008", "mobvalue" -> "4")),
        "个推", "20200502"
      ),
      (Encrypt(SHA256ID).compute("13325789093"), "phonesha256",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2")),
        "个推", "20200502"
      ),
      (Encrypt(SHA256ID).compute("13325789096"), "phonesha256",
        Array(Map("mobid" -> "D004", "mobvalue" -> "0"), Map("mobid" -> "D005", "mobvalue" -> "2")),
        "个推", "20200502"
      ),
    ).toDF(schema: _*)
    insertDF2Table(df1, HIVE_TABLE_LABEL_RELATION, Some("day"), false)

    val schema2 = spark.table(HIVE_TABLE_EXT_LABEL_RELATION_FULL).schema.fieldNames
    val df2 = Seq(
      (Map(MD5ID -> Encrypt(MD5ID).compute("13325789097")), Seq("phonemd5"),
        Map("1_1000" -> "1", "3_1000" -> "3"), "20200501", "个推"),
      (Map(SHA256ID -> Encrypt(SHA256ID).compute("13325789098")), Seq("phonesha256"),
        Map("1_1000" -> "0", "4_1000" -> "4"), "20200501", "个推"),
    ).toDF(schema2: _*)
    insertDF2Table(df2, HIVE_TABLE_EXT_LABEL_RELATION_FULL, Some("channel, day"), false)

  }

  test("test ExtLableRelation") {
    import org.apache.spark.sql.functions._
    val day = "20200502"
    val p = Params(day)
    new ExtLableRelation(spark, p).run()

    val df = spark.table(HIVE_TABLE_EXT_LABEL_RELATION_FULL).where($"day" === day)
    df.show(false)
    assertResult(8)(df.count())

    def actual(seq: Seq[String]): Long = df.where($"type" <=> array(seq.map(lit): _*)).count()

    assertResult(1)(actual(Seq("phone", "phonemd5", "phonesha256")))
    assertResult(2)(actual(Seq("phone", "phonemd5")))
    assertResult(1)(actual(Seq("phone", "phonesha256")))
    assertResult(1)(actual(Seq("phone")))
    assertResult(1)(actual(Seq("phonemd5")))
    assertResult(2)(actual(Seq("phonesha256")))
  }
}
