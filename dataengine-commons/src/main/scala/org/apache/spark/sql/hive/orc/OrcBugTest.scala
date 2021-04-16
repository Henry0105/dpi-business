package org.apache.spark.sql.hive.orc

import org.apache.spark.sql.SparkSession

/**
 * @author juntao zhang
 */
object OrcBugTest {
  def main(args: Array[String]): Unit = {
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("test-orc-bug")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.table("dm_mobdi_mapping.android_id_full").where($"version" === "20180827.1000").show(false)
    //    spark.sql("select * from dm_mobdi_mapping.android_id_full where version=20180827.1000").show(false)

  }
}
