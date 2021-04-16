package com.mob.dataengine.utils.apppkg2vec

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FlatSpec

/*
  * @author sunyl
  */
class ApppkgTopSimilarityTest extends FlatSpec {

  "func weighted2Vec" should "update left vector" in {
    val v1 = Vectors.dense(Array(0.1, 0.2, 0.3, 0.4, 0.5)).toSparse
    val v2 = Vectors.sparse(7, Array(1, 6), Array(1.0, 3.0)).toSparse
    println(ApppkgTopSimilarity.weighted2Vec(v1, v2))
  }

  "func weighted3Vec" should "update head vector" in {
    val v1 = Vectors.dense(Array(0.1, 0.2, 0.3, 0.4, 0.5)).toSparse
    val v2 = Vectors.sparse(5, Array(1, 3), Array(1.0, 3.0)).toSparse
    val v3 = Vectors.sparse(5, Array(1, 4), Array(1.0, 4.0)).toSparse
    println(ApppkgTopSimilarity.weighted3Vec(v1, v2, v3))
  }
}
