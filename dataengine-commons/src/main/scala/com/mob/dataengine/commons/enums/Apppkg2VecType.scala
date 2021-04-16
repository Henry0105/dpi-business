package com.mob.dataengine.commons.enums

/**
 * @author sunyl
 */
object Apppkg2VecType extends Enumeration {
  type Apppkg2VecType = Apppkg2VecType.Value
  val APP2VEC: Apppkg2VecType = Value(1, "app2vec")
  val ICON2VEC: Apppkg2VecType = Value(2, "icon2vec")
  val DETAIL2VEC: Apppkg2VecType = Value(4, "detail2vec")
}
