package com.mob.dataengine.engine.core.profile.export.bt

import com.mob.dataengine.commons.profile.MetadataUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProfileBatchSplitter(spark: SparkSession, splitThreshold: Int = 20) {
  def trySplitByProfile(resultDF: DataFrame, profileIds: Seq[String]): Option[Map[String, (DataFrame, Seq[String])]] = {
    if (profileIds.length > splitThreshold) {
      val splitInfo = getSplitInfo(profileIds)
      val splitDFs = splitDF(resultDF, splitInfo)
        .filter{ case (_, (df, _)) => df.filter("size(features) > 0").count() > 0}
      Option(splitDFs)
    } else {
      None
    }
  }

  private[bt] def collectProfileIds(retDF: DataFrame): Seq[String] = {
    import org.apache.spark.sql.functions.{explode, col}
    retDF.select(explode(col("features")))
      .selectExpr("split(key, '_')[0]")
      .distinct()
      .collect()
      .map(_.getString(0))
  }

  private def splitDF(df: DataFrame, splitInfo: Map[String, String]): Map[String, (DataFrame, Seq[String])] = {
    // 将map切成多个map
    val profileMap: Map[String, Seq[String]] = splitInfo.toSeq.groupBy(_._2).map{case (key, s) => (key, s.map(_._1))}
    val splitInfoBC = spark.sparkContext.broadcast(splitInfo)
    spark.udf.register("split_map", (m: Map[String, Seq[String]]) => {
      if (null == m || m.isEmpty) { // 有些confidence为null
        null
      } else {
        val tmp: Seq[(String, String, Seq[String])] = m.toSeq.map { case (k, v) =>
          (splitInfoBC.value.getOrElse(k.substring(0, k.indexOf("_")), "unknown"), k, v)
        }
        tmp.groupBy(_._1).mapValues(seq => seq.map { case (_, k, v) => (k, v) }.toMap)
      }
    })

    val tmpDF = df.selectExpr("device", "day", "split_map(features) as new_tags").cache()

    (splitInfo.values ++ Seq("unknown")).toSet[String].map{k =>
      val retDF = tmpDF.selectExpr("device", "day", s"new_tags['$k'] as features")
      if(k == "unknown") {
        (k, (retDF, collectProfileIds(retDF)))
      } else {
        (k, (retDF, profileMap(k)))
      }
    }.toMap
  }

  // 递归地查找分类id对应的1级或2级的分类id
  private def queryParent(map: Map[Int, (Int, Int)], id: Int): Int = {
    val array = map.get(id) match {
      case Some(x) => x
      case _ => throw new Exception(s"$id(profile_category_id) is not in table t_profile_category")
    }
    if (array._1 == 1 || array._1 == 2) {
      id
    } else {
      queryParent(map, array._2)
    }
  }

  private def getSplitInfo(profileIds: Seq[String]): Map[String, String] = {
    val profileId2CategoryIdMap = MetadataUtils.findProfileId2CategoryId(profileIds)
    val categoryMap = MetadataUtils.findProfileCategory().map(obj =>
      (obj.id, (obj.level, obj.parentId))).toMap

    val category2ParentMap: Map[String, String] = profileId2CategoryIdMap.values
      .map(catId => (catId.toString, queryParent(categoryMap, catId.toInt).toString)).toMap

    profileId2CategoryIdMap.map{ case (profileId, categoryId) =>
      (profileId, category2ParentMap(categoryId))
    }
  }
}
