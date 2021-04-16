package com.mob.dataengine.engine.core.jobsparam

import com.mob.dataengine.commons.{BaseParam, JobInput, JobOutput}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

class CrowdFilterInput(override val uuid: String, include: Map[String, String],
  exclude: Map[String, String]) extends JobInput(uuid, 4) with Serializable {
  def hasInput: Boolean = StringUtils.isNotBlank(uuid)

  override def toString: String = {
    s"""
       |${super.toString}, include=$include, exclude=$exclude
     """.stripMargin
  }

  def createFilters(m: Map[String, String]): Map[String, Filter] = {
    m.filter{ case (_, v) => StringUtils.isNotBlank(v) }
      .map{ case (k, v) => (k, new Filter(v.split(",")))}
  }

  val includeFilters: Map[String, Filter] = createFilters(include)
  val excludeFilters: Map[String, Filter] = createFilters(exclude)
}

class CrowdFilterParam(override val inputs: Seq[CrowdFilterInput], override val output: JobOutput)
  extends BaseParam(inputs, output) with Serializable

object CrowdFilterParam {
  val singleValueField: Set[String] = Set("agebin", "edu", "income", "kids", "model_level",
    "occupation", "gender", "car", "country", "province", "city", "permanent_country",
    "permanent_province", "permanent_city", "home_geohash", "work_geohash")

  val intFields: Set[String] = Set("agebin", "edu", "income", "kids", "model_level", "occupation", "gender", "car")

  val latLonList = "lat_lon_list"
}

class Filter(expected: Seq[String]) extends Serializable {
  def filter(r: Row, field: String): Boolean = {
    if (field.equals(CrowdFilterParam.latLonList)) {
      filterLatLon(r, expected.map(_.toString))
    } else {
      if (CrowdFilterParam.intFields.contains(field)) {
        val value = r.getAs[Int](field).toString
        expected.contains(value)
      } else {
        filterStringField(r, field, expected)
      }
    }
  }


  def filterStringField(r: Row, field: String, expected: Seq[String]): Boolean = {
    // hive中数据 7015_022, 作出7015,表示一级分类
    val value = if ("catelist".equals(field)) {
      val tmp = r.getAs[String](field).split(",")
      (tmp ++ tmp.filter(_.length > 4).map(_.substring(0, 4))).toSet.mkString(",")
    } else {
      r.getAs[String](field)
    }

    if (CrowdFilterParam.singleValueField.contains(field)) {
      expected.contains(value)
    } else {
      if (value != null) expected.intersect(value.split(",")).nonEmpty else false
    }
  }

  def filterLatLon(r: Row, expected: Seq[String]): Boolean = {
    val fields = Array("home_geohash", "work_geohash", "frequency_geohash_list")
    val tmp = fields.flatMap{f =>
      r.getAs[String](f) match {
        case null => Seq.empty[String]
        case v => v.split(",")
      }
    }.filter(StringUtils.isNotBlank)

    if (tmp.isEmpty) {
      false
    } else {
      tmp.exists{ v =>
        expected.exists(t => t.startsWith(v.toLowerCase()))
      }
    }
  }
}
