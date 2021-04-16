package com.mob.dataengine.engine.core

import com.mob.dataengine.commons.DeviceSrcReader
import com.mob.dataengine.commons.pojo.ParamInput
import com.mob.dataengine.engine.core.crowd.CrowdSetOptsParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FlatSpec

import scala.collection.mutable

/**
 * @author juntao zhang
 */
class CrowdSetOptsParserTest extends FlatSpec with LocalSparkSession{

  "CrowdSetOptsParser" should "parse test" in {
    val crowdRDDMap = new mutable.HashMap[String, RDD[String]]()
    crowdRDDMap.put("c1", spark.sparkContext.parallelize(Seq("1", "2", "4"), 2))
    crowdRDDMap.put("c2", spark.sparkContext.parallelize(Seq("5", "2", "4"), 2))
    crowdRDDMap.put("c3", spark.sparkContext.parallelize(Seq("5", "4"), 2))
    val res = CrowdSetOptsParser("c1 | c2  - c3", crowdRDDMap).run()
    res.foreach(r => {
      assert(r.collect().mkString(",") == "1,2")
    })
  }

  "CrowdSetOptsParser" should "empty parse test" in {
    val crowdRDDMap = new mutable.HashMap[String, RDD[String]]()
    crowdRDDMap.put("1", spark.sparkContext.emptyRDD)
    crowdRDDMap.put("2", spark.sparkContext.parallelize(Seq("5", "2", "4"), 2))
    val res = CrowdSetOptsParser("1 | 2", crowdRDDMap).run()
    res.foreach(r => {
      assert(r.collect().sorted.mkString(",") == "2,4,5")
    })
  }

  "CrowdSetOptsParser1" should "parse test2" in {
    val crowdRDDMap = new mutable.HashMap[String, RDD[String]]()
//    crowdRDDMap.put("c1", DeviceSrcReader.toRDD(spark,
//      ParamInput(None, Some("c1"),
//        "http://10.21.131.11:20101/fs/download?path=dataengine/demo/sample1.tar.gz&module=dataengine",
//        Some("dfs"), Some("tgz"), None)))
//    crowdRDDMap.put("c2", DeviceSrcReader.toRDD(spark,
//      ParamInput(None, Some("c2"),
//        "http://10.21.131.11:20101/fs/download?path=dataengine/demo/sample2.txt&module=dataengine",
//        Some("dfs"), None, None)))
//    val res = CrowdSetOptsParser("c1 - c2", crowdRDDMap).run()
//    res.foreach(r => {
//      println(r.collect().mkString(","))
//    })
  }
}

