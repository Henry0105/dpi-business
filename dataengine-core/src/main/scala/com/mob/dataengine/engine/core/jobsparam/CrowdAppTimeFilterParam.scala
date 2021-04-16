package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.enums.InputType.InputType
import com.mob.dataengine.commons.enums.{AppStatus, InputType}
import com.mob.dataengine.commons.{BaseParam2, JobInput2, JobOutput2}
import com.mob.dataengine.core.utils.DataengineException
import com.mob.dataengine.engine.core.crowd.utils.CrowdExceptionMessage


case class CrowdAppTimeFilterInputRule(include: Option[Int]
                                       , appType: Option[Int] = None
                                       , appList: Option[Seq[String]]
                                       , appStatus: Option[Int]
                                       , numberRestrict: Option[String]
                                       , timeInterval: Option[Seq[String]]
                                      ) {

  val includeRule: Boolean = include match {
    case Some(0) => false
    case Some(_) => true
    case x@_ => throw DataengineException(CrowdExceptionMessage.errorMsg("include", x), 1)
  }

  val appStatusRule: String = appStatus match {
    case Some(x) if Seq(-1, 0, 1, 3).contains(x) => AppStatus.withId(x).toString
    case x@_ => throw DataengineException(CrowdExceptionMessage.errorMsg("appStatus", x), 1)
  }

  def appListRule(inputTypeEnum: InputType, appListSep: Option[String]): Option[Seq[String]] = appList match {
    case xs@Some(_) => xs
    case None if InputType.isEmpty(inputTypeEnum) =>
      throw DataengineException(CrowdExceptionMessage.errorMsg("appList_empty", None), 1)
    case None if appListSep.isDefined => None
    case None => throw DataengineException(CrowdExceptionMessage.errorMsg("appList", None), 1)
  }

  val appTypeRule: String = appType match {
    case Some(1) => "app_name"
    case Some(2) => "pkg"
    case x@_ => throw DataengineException(CrowdExceptionMessage.errorMsg("appType", x), 1)
  }

  var timeIntervalRule: (String, String) = timeInterval match {
    case Some(x) if x.length == 2 => (x.head, x(1))
    case x@_ => throw DataengineException(CrowdExceptionMessage.errorMsg("timeInterval", x), 1)
  }

  val numberRestrictRule: (Int, Int) = numberRestrict match {
    case Some("0") => (1, -1)
    case Some("1") => (-1, -1)
    case Some(x) =>
      try {
        val splits = x.replaceAll("\"", "").split(",")
        (splits.head.toInt, splits(1).toInt)
      } catch {
        case _: Exception => throw DataengineException(CrowdExceptionMessage.errorMsg("numberRestrict", Some(x)), 1)
      }
    case None => throw DataengineException(CrowdExceptionMessage.errorMsg("numberRestrict", None), 1)
  }
}

/**
 * app筛选人群输入参数
 *
 * @param uuid           输入数据的uuid分区 或者 sql语句
 * @param rules          规则集
 * @param appListIndex   app名称/app包名列表的索引(默认2) 类似idx
 * @param appListSep     app名称/app包名列表的分隔符 类似sep
 */
case class CrowdAppTimeFilterInput(override val uuid: String,
                                   override val sep: Option[String] = None,
                                   override val header: Int = 0,
                                   override val idx: Option[Int] = None,
                                   override val headers: Option[Seq[String]] = None,
                                   override val inputType: String = "uuid",
                                   rules: Option[Seq[CrowdAppTimeFilterInputRule]],
                                   appListIndex: Int = 2,
                                   appListSep: Option[String] = None)
  extends JobInput2(uuid = uuid, sep = sep, header = header, idx = idx, headers = headers, inputType = inputType)
    with Serializable {
  override def toString: String =
    s"""
       |${super.toString}, rules=$rules,
       |appListIndex=$appListIndex, appListSep=$appListSep
     """.stripMargin
}

/**
 * app筛选人群输出参数
 *
 * @param uuid       输出的uuid分区
 * @param hdfsOutput 输出hdfs地址
 * @param limit      输出行数限制
 * @param keepSeed   0表示导出的时候不保留种子数据,并且将数据都放在一列中；1表示输出种子数据。默认0
 */
case class CrowdAppTimeFilterOutput(override val uuid: String,
                                    override val hdfsOutput: String = "",
                                    override val limit: Option[Int] = Some(-1),
                                    override val keepSeed: Int = 0)
  extends JobOutput2(uuid, hdfsOutput, limit, keepSeed)


class CrowdAppTimeFilterParam(override val inputs: Seq[CrowdAppTimeFilterInput],
                              override val output: CrowdAppTimeFilterOutput) extends BaseParam2(inputs, output)
  with Serializable {

  val isAppListParam = if (inputs.head.rules.nonEmpty) {
    inputs.head.rules.get.head.appList.isDefined
  } else false




  val appListIndex: Int = inputs.head.appListIndex - 1

  val appListSep: Option[String] = if (isAppListParam || InputType.isEmpty(inputTypeEnum)) {
    None
  } else {
    inputs.head.appListSep match {
      case x@Some(_) if sep.isEmpty || sep.get == x.get =>
        throw DataengineException(CrowdExceptionMessage.errorMsg("appListSep_sep_error", x), 1)
      case x@Some(_) => x
      case x@_ => throw DataengineException(CrowdExceptionMessage.errorMsg("appListSep", x), 1)
    }
  }
}
