package com.mob.dataengine.engine.core.crowd.selection

import org.apache.spark.sql.DataFrame

package object optimization {

  case class SourceData(sourceDF: DataFrame, totalCnt: Long) {
    def isEmpty: Boolean = totalCnt <= 0
  }

}
