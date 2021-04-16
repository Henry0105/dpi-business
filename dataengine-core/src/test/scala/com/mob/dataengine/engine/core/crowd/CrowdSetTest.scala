package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.utils.{CSVUtils, PropUtils}
import com.mob.dataengine.utils.FileUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class CrowdSetTest  extends FunSuite with LocalSparkSession{

  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  val json = FileUtils.getJson("unittest/crowd_set_operation/crowd_set_operation.json")

  override def beforeAll(): Unit = {
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql(sqlText = "CREATE DATABASE rp_dataengine")

    prepareDataOptCache()
  }

  override def afterAll(): Unit = {
    spark.sql(sqlText = "DROP DATABASE IF EXISTS rp_dataengine CASCADE")
  }


  def prepareDataOptCache(): Unit = {
    val sql = FileUtils.getSqlScript("conf/sql_scripts/rp_tables_create/rp_dataengine/data_opt_cache.sql",
        tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
    spark.sql(sql)
    CSVUtils.fromCsvIntoHive(s"hive_data/rp_dataengine/data_opt_cache.csv",
      spark = spark, tableName = PropUtils.HIVE_TABLE_DATA_OPT_CACHE,
      partition = "created_day", "biz", "uuid")
  }

  def checkResult(uuid: String): Unit = {
    import spark.implicits._

    spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
      .where(conditionExpr = s"uuid = '${uuid}'")
      .show(truncate = false)

    val targetDF = Seq(
      ("c1036991ef0f961a2329c5326913a611a98ece6b"),
      ("d103beeb0b1a3a966b406f8cf7be418e933c9de6"),
      ("h1055a737cb4516640b6419c8f3929469e77d18b"),
      ("g1051e8c5391e38461b32f3d108cd816886e7511")
    ).toDF("data")

    val resultDF = spark.table(PropUtils.HIVE_TABLE_DATA_OPT_CACHE)
      .where(conditionExpr = s"uuid = '${uuid}'").select("data")

   assert(resultDF.join(targetDF, resultDF("data") === targetDF("data"), "inner").count() == 4)
  }

  test(testName = "crowd_set_operate_test") {
    CrowdSet.main(Array(json))
    checkResult(uuid = "0f18147bb13ce8b089116c79b3202c31")
  }
}
