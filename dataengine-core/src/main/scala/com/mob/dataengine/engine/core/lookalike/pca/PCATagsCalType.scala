package com.mob.dataengine.engine.core.lookalike.pca

object PCATagsCalType extends Enumeration with Serializable {

  type PCATagsCalType = Value
  /* 全量更新 */
  val FULL: PCATagsCalType.Value = Value(0)
  /* 增量更新 */
  val INCR: PCATagsCalType.Value = Value(1)

}
