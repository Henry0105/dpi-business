package com.mob.dataengine.engine.core.crowd.utils

object CrowdExceptionMessage {

  /** 报错提示信息 */
  def errorMsg(paramName: String, op: Option[Any]): String = {
    val prefix = s"ERROR: 参数($paramName)错误,参数规范"
    val content = paramName match {
      case "include" =>
        "[非空,0为false,1为true]"
      case "appStatus" =>
        "[非空,1(在装)、2(活跃)、3(卸载)、4(新装)]"
      case "appType" =>
        "[非空,1:app名称 2:app包名]"
      case "appList" | "appListSep" =>
        "[没有appList的参数的情况下，appListSep为必填项,同时存在时,优先appList。appListSep配合appListIndex使用。]"
      case "timeInterval" =>
        "[非空,长度为2的数组,[startTime,endTime]]"
      case "numberRestrict" =>
        """[非空,"0","1","*,*"]"""
      case "appList_empty" =>
        """[不传入种子包的模式下参数appList必填]"""
      case "appListSep_sep_error" =>
        """[种子包携带appList时,sep不能为空，且不能与appListSep相等]"""
    }
    val suffix = s"错误输入为:$op"
    s"$prefix$content    $suffix"
  }
}