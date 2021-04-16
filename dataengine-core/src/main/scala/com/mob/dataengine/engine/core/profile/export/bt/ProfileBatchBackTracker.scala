package com.mob.dataengine.engine.core.profile.export.bt

import java.util.{Calendar, Locale}

import com.mob.dataengine.commons.helper.DateUtils
import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.traits.Cacheable
import com.mob.dataengine.commons.utils.{AppUtils, FnHelper, PropUtils}
import org.apache.commons.lang3.time
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.orc.OrcIndexReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author juntao zhang
 */
case class ProfileBatchBackTracker(
  @transient spark: SparkSession,
  @transient inputDF: DataFrame,
  minDay: Option[String] = None,
  profileIds: Seq[String],
  trackDay: Option[String] = None,
  outputUUID: String,
  override val outputHDFSLanguage: String = "en",
  override val limit: Option[Int] = None,
  override val outputHDFS: Option[String] = None,
  warehouse: Option[String] = None
) extends Cacheable with ProfileBatchAbstract {
  lazy val allDays: Seq[String] = inputDF.select("day").distinct().collect().map(_.getString(0)).sorted
  inIdsFull ++= profileIds.toBuffer

  // 给taglist和catelist 2个虚拟的标签id
  // 这2个标签不是从full表拿的,所以是日更
  val taglistMockProfileId: String = "5919_1000"
  val catelistMockProfileId: String = "5920_1000"
  val tagcateMockProfileIds: Seq[String] = Seq(taglistMockProfileId, catelistMockProfileId)
  // 专门支持金融线ra标签的312个标签
  val raProfileIds: Seq[String] = 7793.to(8104).map(s => s"${s}_1000")
  // 融慧:
  // 6156-6173: 融慧产品1
  // 6174-6181: 融慧DNN
  val rhProfileIds: Seq[String] = 6156.to(6181).map(s => s"${s}_1000")
  val rhCp7ProfileIds: Seq[String] = 8117.to(8120).map(s => s"${s}_1000")
  val rhCp2ProfileIds: Seq[String] = 8109.to(8116).map(s => s"${s}_1000")
  val jxProfileIds: Seq[String] = Seq("8133_1000")

  // 金融定制标签回溯
  val mobfinProfileInfo = Seq(TrackProfileInfo(raProfileIds,
    PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V3, "ra"),
    TrackProfileInfo(rhProfileIds, PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V4, "rh"),
    TrackProfileInfo(rhCp7ProfileIds, PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_RONGHUI_PRODUCT7, "rh_cp7"),
    TrackProfileInfo(rhCp2ProfileIds, PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_RONGHUI_PRODUCT2, "rh_cp2"),
    TrackProfileInfo(jxProfileIds, PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_JX, "jx")
  )
  // 金融定制标签全部
  val mobinProfileIds: Seq[String] = mobfinProfileInfo.map(_.profileIds).reduce(_ union _)

  // 获取标签信息, 标签id -> [标签更新周期,标签更新日]
  val profileIdPeriod: Map[String, (String, Int)] = MetadataUtils.findTrackPeriodInfo()
  val profileIdPeriodBC: Broadcast[Map[String, (String, Int)]] = spark.sparkContext.broadcast(profileIdPeriod)

  import spark.implicits._



  def start(): Unit = {
    logger.info(
      s"""
         |days is $allDays
         |minDay is $minDay
         |tagIds:\n${profileIds.mkString("\n")}
      """.stripMargin)
    sql("set org.apache.spark.sql.autoBroadcastJoinThreshold=109715200") // 100M
    spark.udf.register("agg_map_fun", new MapsAggFunction())
    spark.udf.register("latest_feature", new LatestFeatureFunction(profileIds ++ tagcateMockProfileIds))
  }

  def cal(): Unit = {
    val finalTableSchema: StructType = StructType(Seq(
      StructField("device", StringType),
      StructField("day", StringType),
      StructField("features", MapType(
        StringType,
        ArrayType(StringType)
      )),
      StructField("data", StringType)
    ))
    // device为空,直接跳出cal()
    if (0L == inputDF.count) {
      finalDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], finalTableSchema)
      return
    }
    cacheImmediately(inputDF.map { r =>
      val device = r.getAs[String]("device")
      val day = r.getAs[String]("day")
      val data = r.getAs[String]("data")

      val startDay = trackDay.getOrElse(day)
      val newStartDay = calNewStartDayByProfileIds(startDay, profileIds)
      val endDay = atMonthsBefore(newStartDay, 3)
      if (trackDay.isDefined) {
        // 当trackDay有传时，用minDay作为回溯的结束日期,这种情况下minDay有默认值为 trackDay-3months
        (device, day, newStartDay, minDay.getOrElse(endDay), data)
      } else {
        // 当trackDay没有传时，minDay就不使用，而是用种子包里的日期-3个月，作为回溯的结束日期
        (device, day, newStartDay, endDay, data)
      }
    }.toDF(
      "device", "old_day", "day", "backtracking_day", "data"
    ), "t1")


    val taglistTagId2ProfileIdMap: Map[String, String] = MetadataUtils.findTaglistLikeProfiles("tag_list;")
    val catelistTagId2ProfileIdMap: Map[String, String] = MetadataUtils.findTaglistLikeProfiles("catelist;")
    val taglistProfileIds: Seq[String] = taglistTagId2ProfileIdMap.values.toSeq
    val catelistProfileIds: Seq[String] = catelistTagId2ProfileIdMap.values.toSeq
    val v2v3ProfileIds: Seq[String] = MetadataUtils.findV2V3OnlineProfiles()

    // 其他标签
    val otherProfileIds = profileIds.diff(v2v3ProfileIds.union(mobinProfileIds))
    // v6缩减标签
    val v6ProfileIds = profileIds.diff(mobinProfileIds)

    //  mockProfileId -> map(profile_id -> tagId)
    val tagcateMockInfo: Map[String, Map[String, String]] = Map(
      taglistMockProfileId -> taglistTagId2ProfileIdMap,
      catelistMockProfileId -> catelistTagId2ProfileIdMap
    )

    // 获取回溯的最早和最晚日期, 来确定要取的数据的分区, 因为数据是按年分区的
    val r = sql(
      s"""
         |select max(day) max_day, min(backtracking_day) min_day
         |from t1
        """.stripMargin).first()

    val partitionMaxDay = r.getAs[String]("max_day")
    val partitionMinDay = r.getAs[String]("min_day")

    println("days-span: ", partitionMinDay, partitionMaxDay)
    // 由于做了标签缩减, 20210101(包含)以后的标签都走profile_day_v6
    val profileDayBounder = AppUtils.PROFILE_BOUND_DAY
    // 这个取前一天
    val beforeProfileDayBounder = DateUtils.minusDays(profileDayBounder, 1)
    // 表示是否走老表
    val useOldProfileDay = partitionMinDay < profileDayBounder
    // 表示是否走新表
    val useNewProfileDay = partitionMaxDay >= profileDayBounder

    val hasTaglist = if (profileIds.intersect(taglistProfileIds ++ catelistProfileIds).nonEmpty) {
      println("包含taglist/catelist")
      true
    } else {
      false
    }

    val resultBuffer = ArrayBuffer.empty[DataFrame]

    // online的v2/v3标签只走老的索引表, 其他的都走v2的索引表
    if (profileIds.intersect(v2v3ProfileIds).nonEmpty && useOldProfileDay) {
      println("处理v2/v3标签")
      resultBuffer.append(
        queryTrackData(inputDF, PropUtils.HIVE_TABLE_PROFILE_HISTORY_INDEX,
          PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY,
          profileIds.diff(taglistProfileIds ++ catelistProfileIds), "norm", partitionMinDay,
          smallerDay(partitionMaxDay, beforeProfileDayBounder))
      )
    }

    mobfinProfileInfo.foreach{ mpi =>
      if (mpi.profileIds.intersect(profileIds).nonEmpty) {
        println(s"处理${mpi.name}标签")
        resultBuffer.append(
          queryTrackData(inputDF, PropUtils.HIVE_TABLE_PROFILE_HISTORY_INDEX,
            mpi.dataTable,
            mpi.profileIds.intersect(profileIds), mpi.name, partitionMinDay, partitionMaxDay))
      }
    }

    if (otherProfileIds.nonEmpty && useOldProfileDay) {
      println("处理taglist,catelist和其他标签,包括4个维度标签")
      val v2IndexProfileIds = if (hasTaglist) {
        tagcateMockProfileIds ++ otherProfileIds
      } else {
        otherProfileIds
      }

      resultBuffer.append(
        queryTrackData(inputDF, PropUtils.HIVE_TABLE_PROFILE_HISTORY_INDEX,
          PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V2, v2IndexProfileIds, "tag", partitionMinDay,
          smallerDay(partitionMaxDay, beforeProfileDayBounder)))
    }

    if (v6ProfileIds.nonEmpty && useNewProfileDay) {
      println("走新标签表")
      val v6IndexProfileIds = if (hasTaglist) {
        tagcateMockProfileIds ++ v6ProfileIds
      } else {
        v6ProfileIds
      }

      resultBuffer.append(
        queryTrackData(inputDF, PropUtils.HIVE_TABLE_PROFILE_HISTORY_INDEX,
          PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V6, v6IndexProfileIds, "v6",
          biggerDay(partitionMinDay, profileDayBounder),
          partitionMaxDay))
    }

    // 将所有获取的数据拿出来进行日期的过滤
    resultBuffer.reduce(_ union _).map{ r =>
      val device = r.getAs[String]("device")
      val oldDay = r.getAs[String]("old_day")
      // val day = r.getAs[String]("day")
      val features = r.getAs[Map[String, Seq[String]]]("features")
      val data = r.getAs[String]("data")
      val newFeatures = features.filter{ case (profileId, arr) =>
        // 这里需要种子包里面的day再算一次日期
        val startDay = calNewStartDay(oldDay, profileId)
        val endDay = atMonthsBefore(startDay, 3)
        arr.head <= startDay && arr.head >= endDay
      }
      (device, oldDay, newFeatures, data)
    }.toDF("device", "day", "features", "data")
      .where("size(features) > 0").createOrReplaceTempView("t3")

    finalDF = cacheImmediately(sql(
      s"""
        |select device,day,latest_feature(features) as features,first(data) as data
        |from t3
        |group by device,day
       """.stripMargin), s"t4")

    finalDF = if (hasTaglist) {
      explodeTaglistLike(finalDF, tagcateMockInfo,
        (taglistProfileIds ++ catelistProfileIds).intersect(profileIds).toSet).cache()
    } else {
      finalDF
    }

    logger.info(
      s"""
         |
         |finalDF=>
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |${showString(finalDF, truncate = 40)}
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |
      """.stripMargin)
  }

  def biggerDay(d1: String, d2: String): String = {
    if (d1 > d2) {
      d1
    } else {
      d2
    }
  }

  def smallerDay(d1: String, d2: String): String = {
    if (d1 < d2) {
      d1
    } else {
      d2
    }
  }

  /**
   * http://j.mob.com/browse/MDML-1536
   * 由于回溯时客户传的日期是一个未来的日期,所以取数据的时候,不能取当前更新周期里的数据,要再往前推一个周期
   * 这里将重新计算出每个device下每个标签的开始和结束日期
   * DI的月标签是放在下一个月的分区里面的
   * 标签系统里面没有融慧的标签, 融慧的数据都是月数据放在月末的最后一天
   */

  /**
   * 根据客户的日期和标签,重新计算该标签的起始日期
   * @param day: 客户传的日期
   * @param profileId: 标签ID
   */
  def calNewStartDay(day: String, profileId: String): String = {
    if (mobinProfileIds.contains(profileId)) {
      // 属于月标签,并且数据放在当月的月末最后一天
      // 将day往前推一个月,并取月末
      DateUtils.getLastMonth(day).last
    } else {
      profileIdPeriodBC.value.get(profileId) match {
          // 日更取前一天
        case Some(("day", _)) => DateUtils.minusDays(day, 1)
        case Some(("week", x)) => // 周更标签,数据放在下一个周期的数据里面,处理逻辑类似月更标签
          val dayOfWeek = DateUtils.getDate(day).getDayOfWeek.getValue
          if (dayOfWeek == x) {
            DateUtils.minusDays(day, 1)
          } else {
            day  // 这里就不往前推到特定的天了
          }
        case Some(("month", x)) => // 月更标签,数据放在下一个月的某一天
          // 例如月更是每月16号,那么20200115/20200116应该就只能查到20191216的分区
          val dayOfMonth = DateUtils.getDate(day).getDayOfMonth
          if (dayOfMonth == x) { // 直接到上一个月
            DateUtils.minusMonths(day, 1)
          } else {
            day
          }
        case None => DateUtils.minusDays(day, 1)
      }
    }
  }

  def calNewStartDayByProfileIds(day: String, profileIds: Seq[String]): String = {
    profileIds.map(calNewStartDay(day, _)).max
  }

  /**
   * 因为索引表按照年进行的分区, 分区示例如下:
   * table=rp_mobdi_app.timewindow_online_profile_day/version=2017.1000
   * table=rp_mobdi_app.timewindow_online_profile_day/version=2018.1000
   * table=rp_mobdi_app.timewindow_online_profile_day/version=2019.1000
   * table=rp_mobdi_app.timewindow_online_profile_day/version=2020.1000
   * table=rp_mobdi_app.timewindow_online_profile_day/version=2020.1002
   * 取分区的规则: [每个年份取最大的分区]
   *
   * @param indexTable: 索引表
   * @param dataTable: 数据表
   * @param partitionMinDay: 最小日期, 用来过滤分区表
   * @param partitionMaxDay: 最大日期
   */
    def getSourceTable(indexTable: String, dataTable: String,
    partitionMinDay: String, partitionMaxDay: String): DataFrame = {
    partitionMinDay.substring(0, 4).toInt.to(partitionMaxDay.substring(0, 4).toInt).map{ year =>
      try {
        val version = sql(s"show partitions $indexTable").collect().map { r =>
          r.getString(0)
        }.filter(_.contains(dataTable + "/")).filter(_.contains(year.toString)).map(_.split("=")(2)).max
        println(version)
        sql(
          s"""
             |select device,feature_index
             |from $indexTable
             |where version='$version' and table='$dataTable'
           """.stripMargin)
      } catch {
        case _: Throwable => spark.createDataFrame(
          spark.sparkContext.emptyRDD[Row].setName("empty_source"), spark.table(indexTable).schema)
          .select("device", "feature_index")
      }
    }.reduce(_ union _)
  }

  /**
   * @param inputDF: 输入的种子数据
   * @param indexTable: 索引表
   * @param dataTable: 数据表
   * @param newProfileIds: 处理之后的profileId, 根据是否需要taglist/catelist来进行筛选
   * @param partitionMinDay: 最小日期, 用来过滤分区表
   * @param partitionMaxDay: 最大日期
   */

  def queryTrackData(inputDF: DataFrame, indexTable: String, dataTable: String,
    newProfileIds: Seq[String], tbPrefix: String, partitionMinDay: String, partitionMaxDay: String): DataFrame = {
    val schema: StructType = StructType(Seq(
      StructField("device", StringType),
      StructField("old_day", StringType),
      StructField("day", StringType),
      StructField("backtracking_day", StringType),
      StructField("feature_index", MapType(
        StringType,
        StructType(Array(
          StructField("file_name", StringType),
          StructField("row_number", LongType)
        ))
      )),
      StructField("data", StringType)
    ))

    val encoder: Encoder[Row] = RowEncoder(schema)

    tryBloomLoad(getSourceTable(indexTable, dataTable, partitionMinDay, partitionMaxDay), inputDF)
      .createOrReplaceTempView(s"${tbPrefix}_t2")
    println(s"$dataTable [$partitionMinDay, $partitionMaxDay]")
    val sqlString =
      s"""
         |select /*+MAPJOIN(t1)*/
         |    t1.device,
         |    t1.old_day,
         |    t1.day,
         |    t1.backtracking_day,
         |    ${tbPrefix}_t2.feature_index,
         |    t1.data
         |  from t1
         |  left join ${tbPrefix}_t2
         |    on t1.device = ${tbPrefix}_t2.device
         """.stripMargin
    val tmp = sql(sqlString)

    val tmpDF = tmp.filter($"feature_index".isNotNull).map { r =>
      val device = r.getAs[String]("device")
      val oldDay = r.getAs[String]("old_day")
      val day = r.getAs[String]("day")
      val backtrackingDay = r.getAs[String]("backtracking_day")
      val featureIndex = r.getMap[String, Row](4).filter { case (k, _) =>
        k >= backtrackingDay && k <= day
      }
      val data = r.getAs[String]("data")
      Row(device, oldDay, day, backtrackingDay, featureIndex, data)
    }(encoder).toDF(
      "device", "old_day", "day", "backtracking_day", "feature_index", "data"
    ).filter { r => r.getMap[String, Row](4).nonEmpty }.repartition(16)

    val input = tmpDF.flatMap { r =>
      val device = r.getAs[String]("device")
      val oldDay = r.getAs[String]("old_day")
      val day = r.getAs[String]("day")
      val featureIndex = r.getAs[Map[String, Row]]("feature_index")
      val data = r.getAs[String]("data")
      featureIndex.map { case (k, v) =>
        (device, oldDay, day, s"day=$k/" + v.getAs[String]("file_name"),
          v.getAs[Long]("row_number"), data)
      }
    }.toDF("device", "old_day", "day", "file", "row_number", "data").cache()
    logger.info(
      s"""
         |
         |input=>
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |${showString(input, truncate = 40)}
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |
      """.stripMargin)

    OrcIndexReader(spark,
      input, dataTable, warehouse
    ).search(newProfileIds.toSet).repartition(FnHelper.getCoalesceNum(inputDF.count(), 300)).cache()

//    output.createOrReplaceTempView(s"${tbPrefix}_t3")
//
//    cacheImmediately(sql(
//      s"""
//        |select device,day,latest_feature(features) as features
//        |from ${tbPrefix}_t3
//        |group by device,day
//      """.stripMargin), s"${tbPrefix}_t4")
  }

  /**
   * 对taglist/catelist的数据进行展开, 只支持  720,378,463,584=0.9074,0.6835,0.4221,0.2737 这样的数据结构
   * @param df: device, day, features(map(mockProfileId -> array(fileDay, value)))
   * @param info: mockProfileId -> map(profile_id -> tagId)
   * @param requiredIds: 需要的taglist/catelist 的表情, 可能不是全量的
   * @return 将features中的
   */
  def explodeTaglistLike(df: DataFrame, info: Map[String, Map[String, String]], requiredIds: Set[String]): DataFrame = {
    // 对每行记录做处理
    // 返回: profileId -> array(fileDay, value)
    // 这里要对taglist/catelist再根据传入的profile_ids进行过滤, 因为这里拿到的其实是全部的taglist的数据
    val requireIdsBC = spark.sparkContext.broadcast(requiredIds)
    def explode(day: String, taglist: String, tagId2ProfileIdMap: Map[String, String]): Map[String, Array[String]] = {
      val arr = taglist.split("=")
      arr(0).split(",").zip(arr(1).split(",")).flatMap { case (id, value) =>
        tagId2ProfileIdMap.get(id).filter(requireIdsBC.value.contains)
          .map(profileId => (profileId, Array(day, value)))
      }.toMap
    }

    df.flatMap{ r =>
      val device = r.getAs[String]("device")
      val day = r.getAs[String]("day")
      val features = r.getAs[Map[String, Seq[String]]]("features")
      val data = r.getAs[String]("data")
      val newFeatures: Iterable[Map[String, Array[String]]] = features.map{ case (profileId, arr) =>
        if (info.contains(profileId)) {  // 对taglist进行展开
          explode(arr(0), arr(1), info(profileId))
        } else {
          Map(profileId -> arr.toArray)
        }
      }.filter(_.nonEmpty)

      if (newFeatures.nonEmpty) {
        Some((device, day, newFeatures.reduce(_ ++ _), data))
      } else {
        None
      }
    }.toDF("device", "day", "features", "data")
  }

  def atDaysBefore(day: String, n: Int): String = {
    val c = Calendar.getInstance(Locale.CHINA)
    c.setTimeInMillis(time.DateUtils.parseDate(day, "yyyyMMdd").getTime)
    c.add(Calendar.DAY_OF_MONTH, -1 * n)
    DateFormatUtils.format(c.getTime.getTime, "yyyyMMdd")
  }

  def atMonthsBefore(day: String, n: Int): String = {
    val c = Calendar.getInstance(Locale.CHINA)
    c.setTimeInMillis(time.DateUtils.parseDate(day, "yyyyMMdd").getTime)
    c.add(Calendar.MONTH, -1 * n)
    DateFormatUtils.format(c.getTime.getTime, "yyyyMMdd")
  }

  class LatestFeatureFunction(profileIds: Seq[String]) extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      StructType(Array(
        StructField("feature", MapType(StringType, ArrayType(StringType)))
      ))
    }

    override def bufferSchema: StructType = new StructType().add(
      StructField("buffer", MapType(StringType, ArrayType(StringType)))
    )

    override def dataType: MapType = MapType(StringType, ArrayType(StringType))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, mutable.HashMap[String, Seq[String]]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      merge(buffer, input)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val m1 = buffer1.getMap[String, Seq[String]](0)
      val m2 = buffer2.getMap[String, Seq[String]](0)
      val tmp = new mutable.HashMap[String, Seq[String]]()
      profileIds.foreach { k =>
        val v1 = m1.get(k)
        val v2 = m2.get(k)
        if (v1.isDefined && v2.isDefined) {
          if (v1.get.head > v2.get.head) {
            tmp.put(k, v1.get)
          } else {
            tmp.put(k, v2.get)
          }
        } else if (v1.isDefined) {
          tmp.put(k, v1.get)
        } else if (v2.isDefined) {
          tmp.put(k, v2.get)
        }
      }
      buffer1.update(0, tmp)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getMap[String, Seq[String]](0)
    }
  }
}

/**
 * @param profileIds: 标签id
 * @param dataTable: 数据表名称
 * @param name: 临时表名
 */
case class TrackProfileInfo(profileIds: Seq[String], dataTable: String, name: String)