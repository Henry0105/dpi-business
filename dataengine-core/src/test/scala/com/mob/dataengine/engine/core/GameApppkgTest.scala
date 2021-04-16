package com.mob.dataengine.engine.core

import com.mob.dataengine.engine.core.apppkg2vec.{AppInfo, SimilarityResult, TopNSimilarity}
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class GameApppkgTest extends FunSuite with LocalSparkSession  {
  test("test_json") {
    val seq = Seq(TopNSimilarity(1, Seq(AppInfo("appName", "icon", "apppkg", 0.11))))
    val res = SimilarityResult("test", seq)
    println(res.toJsonString)
  }
}
