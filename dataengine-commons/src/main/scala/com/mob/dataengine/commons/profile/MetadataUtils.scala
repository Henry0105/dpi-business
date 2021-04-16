package com.mob.dataengine.commons.profile

import com.mob.dataengine.commons.utils.JdbcTools
import com.mob.dataengine.utils.PropUtils

/**
 * todo 未来需要和标签系统打通
 * 操作标签系统
 *
 * 直接获取:
 * rp_device_financial_slope_week_profile
 * rp_device_financial_active_week_profile
 * rp_device_financial_active_month_profile
 * rp_device_financial_install_profile
 * rp_device_financial_installed_profile
 *
 * feature字段
 * timewindow_multiloan_finance
 * timewindow_online_profile
 *
 * todo 标签系统还不支持
 * device_install_app_count
 *
 * @author juntao zhang
 */
case class IndividualProfile(
  id: String,
  version: String,
  database: String,
  table: String,
  valueColName: String,
  valueColType: String,
  filterColsString: String // 过滤条件
) {
  val fullId = s"${id}_$version"
  val dbTable = s"${database}.${table}"
  val filterCols: Array[(String, String)] = {
    if (filterColsString.isEmpty) {
      Array()
    } else {
      filterColsString.split("and").flatMap(_.split(";")).map { p =>
        val Array(k, v) = p.split("=")
        (k.trim, v.trim)
      }
    }
  }

  val sqlFilter: String = {
    if (filterCols.nonEmpty) {
      filterCols.map(p => s"${p._1}=${p._2}").mkString("(", " and ", ")")
    } else {
      ""
    }
  }

  val sqlFilterExceptFeature: String = {
    if (filterCols.exists(_._1 != "feature")) {
      filterCols.filter(_._1 != "feature").map(p => s"${p._1}=${p._2}").mkString("(", " and ", ")")
    } else {
      ""
    }
  }

  val sqlFilterFeature: Seq[(String, String)] = {
    filterCols.filter(_._1 == "feature")
  }

  val key: String = (
    Seq(database, table, valueColName) ++ filterCols.sortBy(_._1).map(p => p._2.stripSuffix("'").stripPrefix("'"))
    ).mkString("-")
}

case class ProfileCategory(id: Int, name: String, level: Int, parentId: Int)

object MetadataUtils {
  val jdbcTools = JdbcTools(
    PropUtils.getProperty("tag.mysql.ip"),
    PropUtils.getProperty("tag.mysql.port").toInt,
    PropUtils.getProperty("tag.mysql.database"),
    PropUtils.getProperty("tag.mysql.user"),
    PropUtils.getProperty("tag.mysql.password")
  )

  /**
   * 获取标签列中文
   *
   * @param profileIds profileId_version
   * @return
   */
  def findProfileName(profileIds: Seq[String]): Map[String, String] = {
    val ids = profileIds.map(_.split("_")).map(_ (0))
    jdbcTools.find(
      s"""
         |select profile_id,profile_name from ${jdbcTools.db}.t_profile_metadata
         |where profile_id in ${ids.mkString("(", ",", ")")}
      """.stripMargin, rs => {
        (rs.getString("profile_id"), rs.getString("profile_name"))
      }
    ).toMap
  }

  /**
   * 获取标签中文值
   *
   * @param profileIds profileId_version
   */
  def findProfileValue(profileIds: Seq[String]): Map[String, String] = {
    val filters = profileIds.map(_.split("_")).map(a => s"(profile_id=${a(0)} and profile_version_id=${a(1)})")

    jdbcTools.find(
      s"""
         |select profile_id,profile_version_id,profile_value,profile_value_cn
         | from ${jdbcTools.db}.t_profile_value
         |where profile_value_cn is not null and ${filters.mkString("(", " or ", ")")}
      """.stripMargin, rs => {
        (
          s"${rs.getString("profile_id")}_${rs.getString("profile_version_id")}_${rs.getString("profile_value")}",
          rs.getString("profile_value_cn")
        )
      }
    ).toMap
  }

  /**
   * 获取表元信息
   *
   * @param profileIds profileId_version
   */
  def findIndividualProfile(profileIds: Seq[String], whereExpr: Option[String] = None): Seq[IndividualProfile] = {
    val filters = profileIds.map(_.split("_")).map(a => s"(profile_id=${a(0)} and profile_version_id=${a(1)})")
    jdbcTools.find(
      s"""
         |select * from ${jdbcTools.db}.t_individual_profile
         |where ${filters.mkString("(", " or ", ")")}
         |      ${if (whereExpr.isEmpty) "" else "AND"}
         |      ${whereExpr.getOrElse("")}
      """.stripMargin, rs => {
        val arr = rs.getString("profile_column").split(";", 2)
        IndividualProfile(
          id = rs.getLong("profile_id").toString,
          version = rs.getLong("profile_version_id").toString,
          database = rs.getString("profile_database"),
          table = rs.getString("profile_table"),
          valueColName = arr(0),
          valueColType = rs.getString("profile_datatype"),
          filterColsString = if (arr.size > 1) arr(1) else ""
        )
      })
  }

  /**
   * 获取置信度表元信息
   */
  def findProfileConfidence(profileIds: Seq[String], whereExpr: Option[String] = None): Seq[IndividualProfile] = {
    val filters = profileIds.map(_.split("_")).map(a => s"(profile_id=${a(0)} and profile_version_id=${a(1)})")
    jdbcTools.find(
      s"""
         |select * from ${jdbcTools.db}.t_profile_confidence
         |where ${filters.mkString("(", " or ", ")")}
         |      ${if (whereExpr.isEmpty) "" else "AND"}
         |      ${whereExpr.getOrElse("")}
      """.stripMargin, rs => {
        val arr = rs.getString("profile_column").split(";", 2)
        IndividualProfile(
          id = rs.getLong("profile_id").toString,
          version = rs.getLong("profile_version_id").toString,
          database = rs.getString("profile_database"),
          table = rs.getString("profile_table"),
          valueColName = arr(0),
          valueColType = rs.getString("profile_datatype"),
          filterColsString = if (arr.size > 1) arr(1) else ""
        )
      })
  }

  /**
   * 获取标签类别信息
   */
  def findProfileCategory(): Seq[ProfileCategory] = {
    jdbcTools.find(
      s"""
         |select profile_category_id, profile_category_name,
         |  profile_category_level, profile_category_parent_id
         |from ${jdbcTools.db}.t_profile_category
       """.stripMargin, rs => {
        ProfileCategory(
          rs.getInt("profile_category_id"),
          rs.getString("profile_category_name"),
          rs.getInt("profile_category_level"),
          rs.getInt("profile_category_parent_id")
        )
      })
  }

  /**
   * 获取标签id对应的分类id
   * @param profileIds profileId_version
   */
  def findProfileId2CategoryId(profileIds: Seq[String]): Map[String, String] = {
    val filter = profileIds.map(fullId => fullId.substring(0, fullId.indexOf("_"))).mkString("(", ",", ")")
    jdbcTools.find(
      s"""
         |select profile_id, profile_category_id
         |from ${jdbcTools.db}.t_profile_metadata
         |where profile_id in $filter
       """.stripMargin, rs => {
        (
          rs.getInt("profile_id").toString,
          rs.getInt("profile_category_id").toString
        )
      }).toMap
  }

  /**
   * 获取有置信度标签
   */
  def findConfidenceProfiles(): Seq[String] = {
    jdbcTools.find(
      s"""
         |select profile_id, profile_version_id
         |from ${jdbcTools.db}.t_profile_confidence
         |where profile_id is not null
         |group by profile_id, profile_version_id
       """.stripMargin, rs => {
        val profileId = rs.getInt("profile_id")
        val versionId = rs.getInt("profile_version_id")
        s"${profileId}_$versionId"
      })
  }

  /**
   * 获取类似tag_list和catelist这样存储的所有的profile_id, version_id
   */
  def findTaglistLikeProfiles(prefix: String): Map[String, String] = {
    jdbcTools.find(
      s"""
         |select profile_id, profile_version_id, profile_column
         |from ${jdbcTools.db}.t_individual_profile
         |where profile_column like '$prefix%'
       """.stripMargin, rs => {
        val profileId = rs.getInt("profile_id")
        val versionId = rs.getInt("profile_version_id")
        val id = rs.getString("profile_column").split(";")(1)
        (id, s"${profileId}_$versionId")
      }).toMap
  }

  /**
   * 获取类似v2/v3线上标签
   */
  def findV2V3OnlineProfiles(): Seq[String] = {
    jdbcTools.find(
      s"""
         |select profile_id, profile_version_id
         |from ${jdbcTools.db}.t_individual_profile
         |where profile_table in ('timewindow_online_profile_v2', 'timewindow_online_profile_v3',
         |  'rp_fintech_pre_loan_label', 'device_install_app_count')
       """.stripMargin, rs => {
        val profileId = rs.getInt("profile_id")
        val versionId = rs.getInt("profile_version_id")
        s"${profileId}_$versionId"
      })
  }

  /**
   * 获取MobId -> ProfileId的对应关系
   */
  def findMobId2ProfileMapping(): Map[String, String] = {
    jdbcTools.find(
      s"""
         |select mob_id, profile_id, profile_version_id
         |from v_id_mapping
       """.stripMargin, rs => {
        val mobId = rs.getString("mob_id")
        val profileId = rs.getInt("profile_id")
        val versionId = rs.getInt("profile_version_id")
        (mobId, s"${profileId}_$versionId")
      }).toMap
  }

  /**
   * 获取profile_id, mobid, 中文名称
   */
  def findProfileIdMobIdAndCn(): Seq[(String, String, String)] = {
    jdbcTools.find(
      s"""
         |select mob_id, profile_id, profile_version_id, profile_name
         |from v_id_mapping
       """.stripMargin, rs => {
        val mobId = rs.getString("mob_id")
        val profileId = rs.getInt("profile_id")
        val versionId = rs.getInt("profile_version_id")
        val profileName = rs.getString("profile_name")
        (s"${profileId}_$versionId", mobId, profileName)
      })
  }

  /**
   * 获取 ModId <=> ProfileID 的映射关系
   */
  def findModId2ProfileID(): Seq[(String, String)] = {
    jdbcTools.find(
      s"""
         |select mob_id, profile_id, profile_version_id
         |from ${jdbcTools.db}.v_id_mapping
       """.stripMargin, rs => {
        val mobId = rs.getString("mob_id")
        val profileId = rs.getInt("profile_id")
        val profileVersionId = rs.getInt("profile_version_id")
        (mobId, s"${profileId}_$profileVersionId")
      })
  }

  /**
   * 获取回溯标签对应的更新周期和更新日期
   */
    // todo 这里由于是full表更新是日更,但是在回溯里面是月更,需要做处理
  def findTrackPeriodInfo(): Map[String, (String, Int)] = {
    jdbcTools.find(
      s"""
         |select profile_id, profile_version_id, period, period_day
         |from t_individual_profile
         |where period  in ('month', 'day', 'week')  and os = 'Android'
        """.stripMargin, rs => {
        val profileId = rs.getInt("profile_id")
        val versionId = rs.getInt("profile_version_id")
        val period = rs.getString("period")
        val periodDay = rs.getString("period_day").toInt
        (s"${profileId}_$versionId", (period, periodDay))
      }
    ).toMap
  }
}
