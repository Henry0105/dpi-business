package com.mob.dataengine.commons.enums

object InputType extends Enumeration {

  type InputType = Value
  val EMPTY: InputType = Value(0, "empty")
  val SQL: InputType = Value(1, "sql")
  val DFS: InputType = Value(2, "dfs")
  val UUID: InputType = Value(3, "uuid")

  def isSql(inputType: InputType): Boolean = inputType == SQL
  def isDFS(inputType: InputType): Boolean = inputType == DFS
  def isEmpty(inputType: InputType): Boolean = inputType == EMPTY
  def isDfs(inputType: InputType): Boolean = inputType == DFS
  def isUUID(inputType: InputType): Boolean = inputType == UUID

  def nonEmpty(inputType: InputType): Boolean = !isEmpty(inputType)
}
