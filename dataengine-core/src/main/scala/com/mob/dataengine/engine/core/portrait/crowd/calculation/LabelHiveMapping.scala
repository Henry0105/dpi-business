package com.mob.dataengine.engine.core.portrait.crowd.calculation

import java.io.File

import com.mob.dataengine.engine.core.jobsparam.PortraitCalculationParam
import com.mob.dataengine.utils.FileUtils
import org.apache.commons.io.IOUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

/**
 * @author juntao zhang
 */
case class LabelHiveTableField(
  label: String,
  table: String,
  field: String,
  module: String,
  filter: Option[String],
  subCode: Array[String]
)

case class LabelHiveTable(
  uuid: String,
  elements: Seq[LabelHiveTableField]
) {
  lazy val fields: Seq[String] = elements.map(_.field).distinct
  lazy val fieldLabelMapping: Map[String, Seq[(String, LabelHiveTableField)]] =
    elements.map(t => (t.field, t)).groupBy(_._1)
  lazy val labelFieldMapping: Map[String, Seq[(String, LabelHiveTableField)]] =
    elements.map(t => (t.label, t)).groupBy(_._1)

  def getLabels(field: String): Seq[String] = fieldLabelMapping(field).map(_._2.label)

  def getFilters(field: String): Seq[Option[String]] = fieldLabelMapping(field).map(_._2.filter)

  def isEmpty: Boolean = elements.isEmpty

  def contains(field: String): Boolean = fields.contains(field)

  def select: String = fields.mkString(",")

  override def toString: String = s"uuid:$uuid\n\t\t${elements.mkString("\n\t\t")}"
}

case class LabelHiveTables(
  tableMap: Map[String, Seq[LabelHiveTable]]
) {
  def get(tableName: String): Option[Seq[LabelHiveTable]] =
    tableMap.get(tableName)

  override def toString: String = {
    tableMap.map { case (a, b) => s"$a->\n\t${b.mkString("\n\t")}" }.mkString("\n")
  }
}

object LabelHiveTables {
  implicit val _ = DefaultFormats

  def apply(job: com.mob.dataengine.engine.core.jobsparam.JobContext[PortraitCalculationParam]): LabelHiveTables = {
    val json = IOUtils.toString(
      LabelHiveTables.getClass.getClassLoader.getResourceAsStream("crowd-portrait.json"), "UTF-8")

    val labelFields = JsonMethods.parse(json).extract[Seq[LabelHiveTableField]]
    val labelDetailsMapping = labelFields.groupBy(_.label)
    val table2uuidLHTFsMapping = job.params.flatMap(p => {
      p.inputs.head.tagList.flatMap(tag => {
        labelDetailsMapping.get(tag) match {
          case Some(v) => v.map(lht => p.output.uuid -> lht)
          case None =>
            // 将tag是code_flag_timewindow格式的lable保留
            val splitTag = if (tag.split("_").length >= 2) tag.split("_")(0) else tag
            labelDetailsMapping.get(splitTag) match {
              case Some(v) => v.map(lht => p.output.uuid -> lht)
              case None => throw new IllegalArgumentException(s"No tag [$tag] defined")
            }
        }
      }).distinct
    }).groupBy(_._2.table)
      .map(table2uuidLHTFs => {
        val uuid2lhtMap: Seq[LabelHiveTable] = table2uuidLHTFs._2.groupBy(_._1).mapValues(_.map(_._2)).map(
          uuidLHTs => LabelHiveTable(uuidLHTs._1, uuidLHTs._2)
        ).toSeq
        table2uuidLHTFs._1 -> uuid2lhtMap
      })
    new LabelHiveTables(table2uuidLHTFsMapping)
  }
}
