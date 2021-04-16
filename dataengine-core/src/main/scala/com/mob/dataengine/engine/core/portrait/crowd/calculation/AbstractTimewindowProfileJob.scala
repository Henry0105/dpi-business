package com.mob.dataengine.engine.core.portrait.crowd.calculation

import com.mob.dataengine.commons.annotation.code.{author, createTime}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * 部分金融标签和部分旅游标签计算抽象类, 含标签:
 * F0010, F1036-F1039, LJ000, LL000-LR000
 *
 * @see AbstractJob
 */
@author("yunlong sun")
@createTime("2018-07-04")
abstract class AbstractTimewindowProfileJob(jobContext: JobContext, sourceData: Option[SourceData])
  extends AbstractJob(jobContext, sourceData) {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  import spark._

  /* 指定Flag对应最后分区日期 */
  protected val flag2Day: Map[String, String]

  protected def flag2DayFunc(flagTimeWindowMap: Map[Int, Int]): Map[String, String] = {
    if (needCal) {
      logger.info(s"flag2Day  flagTimeWindowMap=>[$flagTimeWindowMap]")
      val lastPars = sql(s"show partitions $tableName").collect().map(_.getAs[String](0)).toSeq
      flagTimeWindowMap.map(ft => {
        if (lastPars.nonEmpty && genFlagDay(ft._1, ft._2, lastPars)._1 != null) {
          Map(genFlagDay(ft._1, ft._2, lastPars))
        } else {
          Map.empty[String, String]
        }
      }).toSet.flatten.toMap
    } else Map.empty[String, String]
  }

  override protected lazy val originalDFLoadSQL: String =
    lhtOpt.get.flatMap(_.elements.filter(_.filter.isDefined).map {
      e => {
        val filter = e.filter.get
        "(flag=[\\d]+)".r.findFirstIn(filter) match {
          case Some(flag) =>
            if (flag2Day.contains(flag) && fieldsLabelMapping.keys.exists(key => filter.contains(key))) {
              flag -> s"$filter and day='${flag2Day(flag)}'"
            } else {
              null: (String, String)
            }
          case None => null: (String, String)
        }
      }
    }.filter(null != _)).distinct.groupBy(_._1).map(
      filterTuple =>
        s"select device, feature, cnt from $tableName where ${filterTuple._2.map(f => s"(${f._2})").mkString(" or ")}"
    ).mkString(" union all ")


  @transient override protected lazy val finalDF: DataFrame = {
    import spark.implicits._
    joinedDF.groupBy("uuid", "feature")
      .agg(
        sum("cnt").as("sum"),
        countDistinct("device").as("cnt")
      )
      .filter(
        $"feature".isNotNull &&
          trim($"feature").notEqual("")
      )
      .select($"uuid",
        callUDF("toLabel", $"feature").as("label"),
        $"feature".as("label_id"),
        $"cnt", $"sum")
  }


  /* 加载各标签过滤条件 */
  /** private val filters: Seq[String] = {
   * if (needCal) {
   * val filters = lhtOpt.get.flatMap(_.elements.filter(_.filter.isDefined)
   * .map(e => s"(${genFilters(e.filter.get, flag2Day)})")).distinct
   *   logger.info(s"filters=>[${filters.mkString("|")}]")
   * filters
   * } else Seq.empty[String]
   * }
   */
  /* 字段名称标签映射 */
  protected val flagTimeWindowMap: Map[Int, Int]

  // fin03_3_90 -> F1018
  private val fieldsLabelMapping: Map[String, String] = {
    if (needCal) {
      logger.info(s"fieldsLabelMapping  flagTimeWindowMap=>[$flagTimeWindowMap]")
      val map = lhtOpt.get.flatMap(_.elements.filter(_.filter.isDefined)
        .map(lhtf => featureOf(lhtf.filter.get) -> lhtf.label)
        .filter(t => {
          if (StringUtils.isNotBlank(t._1) && "([a-zA-Z0-9]+_[0-9]+_[0-9]+)".r.findFirstIn(t._1).isDefined) {
            StringUtils.isNotBlank(t._1) &&
              flagTimeWindowMap.keys.toList.contains(t._1.split("_")(1).toInt) &&
              flagTimeWindowMap.values.toList.contains(t._1.split("_")(2).toInt)
          } else {
            StringUtils.isNotBlank(t._1)
          }
        }
        )).toMap
      logger.info(s"fieldsLabelMapping=>[$map]")
      map
    } else Map.empty[String, String]
  }

  udf.register("toLabel", toLabel _)

  @transient
  override protected lazy val originalDF: DataFrame = loadOriginalDF
  @transient
  override protected lazy val joinedDF: DataFrame = loadJoinedDF
  @transient
  override protected lazy val groupedDF: DataFrame = emptyDataFrame

  override protected def clear(moduleName: String): Unit = {
    logger.info(s"$moduleName|nothing to clear...")
  }

  /**
   * 根据字段名称返回与之对应的标签名称
   *
   * @param s 输入字段名称
   * @return 对应的标签
   */
  protected def toLabel(s: String): String = fieldsLabelMapping.getOrElse(s, "")

  /**
   * 解析出过滤条件的feature条件
   *
   * @param filter 过滤条件字串
   * @return feature条件
   */
  protected def featureOf(filter: String): String =
    if (StringUtils.isNotBlank(filter)) {
      filter.substring(filter.indexOf("feature") + 9, filter.length - 1)
    }
    else ""

  /**
   * 根据过滤条件中的flag取最近日期的分区
   */
  /** protected def genFilters(filter: String, flag2Day: Map[String, String]): String = {
   * "(flag=[\\d]+)".r.findFirstIn(filter) match {
   * case Some(flag) => s"$filter and day='${flag2Day(flag)}'"
   * case None => ""
   * }
   * }
   */

  /**
   * 获取指定flag的最后分区日期
   */
  def genFlagDay(flag: Int, timewindow: Int, lastPars: Seq[String]): (String, String) = {
    val filterPars = lastPars.filter(p => p.contains(s"flag=$flag") &&
      p.contains(s"timewindow=$timewindow"))
    if (filterPars.nonEmpty) {
      s"flag=$flag" -> filterPars.max.split("\\/")(1).split("=")(1)
    } else {
      (null, null)
    }
  }

  /**
   * 根据crowd-portrait.json配置的label和subCode的关系
   * 得到label和label_id映射关系
   */
  val codeMapping = {
    if (needCal) {
      lhtOpt.get.
        flatMap(_.elements.filter(_.subCode.length > 1))
        .flatMap(table => {
          table.subCode.map(code => {
            (code -> table.label)
          })
        }).toMap
    } else Map[String, String]()
  }

  /**
   * 根据label和label_id映射关系得到label的UDF
   */
  protected def labelid2label(feature: String): String = {
    if (feature == null || feature.isEmpty) {
      ""
    } else {
      val value = feature.split("_")
      if (value.length < 2) "" else {
        codeMapping.get(value(0)) match {
          case Some(m) => m
          case None => ""
        }
      }
    }

  }


  /**
   * 判断label是否为一级分类并且不为空的udf
   */
  lazy val tables = codeMapping.keySet

  protected def isSubAndNotEmpty(feature: String): Boolean = {
    if (feature == null || feature.isEmpty) {
      false
    } else {
      val value = feature.split("_")
      if (value.length < 2) false else tables.contains(value(0))
    }
  }

  udf.register("labelid2label", labelid2label _)
  udf.register("isSubAndNotEmpty", isSubAndNotEmpty _)

  // 计算timewindow_online_profile需要加载的feature,timewindow
  class TableCondition extends Serializable {
    private lazy val hiveTableFields =
      lhts.get(tableName).get.flatMap(_.elements.filter(_.module.equalsIgnoreCase(_moduleName)))

    private lazy val tags = jobContext.job.params.flatMap(p => {
      p.inputs.flatMap(_.tagList)
    }).filter(tag => tag.split("_").length > 2
      && hiveTableFields.map(_.label).contains(tag.split("_")(0))).toArray

    private val features = hiveTableFields.flatMap(_.subCode).toArray
    private val flags = tags.map(_.split("_")(1)).map(flag => {
      if (_moduleName.equals("TimewindowOnlineGameProfile")) {
        flag match {
          case "0" => "0"
          case "1" => "1"
          case "2" => "2"
          case "3" => "3"
          case "4" => "4"
          case _ => "-1"
        }
      } else flag
    })
    private val windows = tags.map(_.split("_")(2))

    private def arraytoString(array: Array[String]): String = {
      val ret = new StringBuffer()
      if (array.length > 0) {
        for (i <- array.indices) {
          if (i == 0) ret.append(s"'${array(i)}'")
          else ret.append(s",'${array(i)}'")
        }
      }
      ret.toString
    }

    override def toString: String = {
      s"""split(feature,'_')[0] in (${arraytoString(features)}) and
         |flag in (${arraytoString(flags)}) and
         |timewindow in (${arraytoString(windows)})
      """.stripMargin
    }
  }


}
