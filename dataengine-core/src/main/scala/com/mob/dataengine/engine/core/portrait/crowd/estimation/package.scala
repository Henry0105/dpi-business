package com.mob.dataengine.engine.core.portrait.crowd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.sketch.BloomFilter


package object estimation {
  case class SourceData(sourceDF: DataFrame, uuid2Count: Broadcast[Map[String, Long]], deviceBF: Broadcast[BloomFilter])

}
