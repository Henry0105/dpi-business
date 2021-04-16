package com.mob.dataengine.engine.core.portrait.crowd.estimation

import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.engine.core.jobsparam.{CrowdPortraitEstimationParam, JobContext}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.{Map, mutable}
import scala.util.Random

/**
 * 垂直画像计算-服装画像(CP002)
 *
 * @param jobContext JobContext任务上下文
 * @param sourceData 输入数据及布隆过滤器广播
 */
@author("yunlong sun")
@createTime("2018-07-24")
case class EstimationClothingScore(jobContext: JobContext[CrowdPortraitEstimationParam], sourceData: Option[SourceData])
  extends AbstractJob(jobContext: JobContext[CrowdPortraitEstimationParam], sourceData) {
  @transient private[this] val logger: Logger = Logger.getLogger(this.getClass)
  /* 服装画像计算对应标签, 与入参对应 */
  override protected lazy val labelName: String = "CP002"
  /* 服装画像计算所需基础标签, 未包含applist(用于购买习惯计算) */
  private[this] val fields = Set("gender", "agebin", "income", "industry", "province_cn", "city_cn")
  val appCate2text = Map(
    "在线购物" -> "关注综合购物网站",
    "团购优惠券" -> "关注折扣信息",
    "跨境电商" -> "关注跨境海淘",
    "品牌电商" -> "关注品牌电商",
    "导购" -> "关注导购信息",
    "生鲜电商" -> "关注综合购物网站",
    "购物分享" -> "关注购物分享"
  )
  val appCate2textBC = jobContext.spark.sparkContext.broadcast(appCate2text)
  val price2code = Map(
    "低档" -> 1,
    "平价" -> 2,
    "中档" -> 3,
    "轻奢" -> 4,
    "奢侈" -> 5
  )
  val price2codeBC = jobContext.spark.sparkContext.broadcast(price2code)
  override protected val tableName: String = PropUtils.HIVE_TABLE_CROWD_PORTRAIT_ESTIMATION_SCORE

  import org.apache.spark.sql.functions._
  import jobContext.spark._
  import jobContext.spark.implicits._
  import jobContext.spark.sparkContext.broadcast

  @transient private lazy val subCateDF = if (needCal) calSubCate() else emptyDataFrame
  @transient private lazy val priceLevelDF = if (needCal) calPriceLevel() else emptyDataFrame

  @transient override protected lazy val originalDF: DataFrame = {
    if (needCal) {
      sourceDF.select("uuid", (fields + "device").toSeq: _*)
        /* 映射不同时期的income数据字典为统一 */
        .withColumn(
        "income_tmp",
        when($"income".equalTo(0) or $"income".equalTo(3), 0)
          .when($"income".equalTo(1) or $"income".equalTo(4) or $"income".equalTo(5), 1)
          .when($"income".equalTo(2) or $"income".equalTo(6) or $"income".equalTo(7), 2)
      )
        .drop("income").withColumnRenamed("income_tmp", "income")
        .groupBy("uuid", fields.toSeq: _*)
        .agg(count("device").as("cnt")).cache()
    } else emptyDataFrame
  }
  /* 数据准备, 用于计算大类别/自类别/价格范围偏好 */
  @transient override protected lazy val groupedDF: DataFrame = {
    if (needCal) {
      originalDF.select("uuid", "gender", "agebin", "income", "industry", "cnt")
        .filter($"agebin".geq(5) and $"agebin".leq(9)
          and $"gender".geq(0) and $"gender".leq(1)
          and $"industry".geq(0) and $"industry".leq(12)
          and $"income".geq(0) and $"income".leq(2))
        .groupBy("uuid", "gender", "agebin", "income", "industry")
        .agg(sum("cnt").as("cnt")).cache()
    } else emptyDataFrame
  }
  /* 和服装分类/系数表进行JOIN */
  @transient override protected lazy val joinedDF: DataFrame = {
    if (needCal) {
      val clothingMegaCateMappingBC = loadClothingMegaCateMapping()
      val clothingCateMappingBC = loadClothingCateMapping()
      groupedDF.flatMap {
        case Row(uuid: String, gender: Int, agebin: Int, income: Int, industry: Int, cnt: Long) =>
//          println(s"print agebin => $agebin, gender => $gender, industry => $industry income = $income" +
//            s" megaCateMappingKey => ${zip3(agebin, gender, industry)}" +
//            s" cateMappingKey => ${zip4(income, zip3(agebin, gender, industry))}" +
//            s" clothingCateMappingBC => ${clothingCateMappingBC.value}" +
//            s" clothingMegaCateMappingBC => ${clothingMegaCateMappingBC.value}")
          val megaCateMappingKey = zip3(agebin, gender, industry)
          val cateMappingKey = zip4(income, megaCateMappingKey)
          val cateMappingValue = clothingCateMappingBC.value(cateMappingKey)
          clothingMegaCateMappingBC.value(megaCateMappingKey).flatMap {
            case (cate, megaCateWeight) =>
              val megaCateCount = cnt * megaCateWeight
              cateMappingValue.map {
                case (cate2, cate3, price, cateWeight) =>
                  val cateCount = cnt * cateWeight
                  (uuid, cate, cate2, cate3, price, cnt, megaCateCount, cateCount)
              }
          }
      }.toDF("uuid", "cate", "cate2", "cate3", "price", "cnt", "mega_cate_cnt", "cate_cnt").cache()
    } else emptyDataFrame
  }

  /* 最终结果, 组合各模块计算结果 */
  @transient override protected val finalDF: DataFrame = {
    if (needCal) {
      jobContext.spark.udf.register("toLabel", toLabel _)
      /* 购买习惯偏好, 编码为2 */
      calPurchaseHabit()
        .union(
          calMegaCate()
            /* 大类别偏好计算, 编码为1 */
            .select($"uuid", lit(labelName).as("label"), lit(1).as("type"),
            $"cate".as("sub_type"), $"mega_cate_cnt".as("cnt"), $"percent")
        )
        .union(
          subCateDF
            /* 子类别偏好计算, 编码为男装/女装/内衣/鞋子/运动/皮具箱包/配饰=>3/6/9/12/15/18/21 */
            .selectExpr("uuid", "'CP002' as label",
            """
              |CASE cate2
              |  WHEN '男装' THEN 3
              |  WHEN '女装' THEN 6
              |  WHEN '内衣' THEN 9
              |  WHEN '鞋子' THEN 12
              |  WHEN '运动' THEN 15
              |  WHEN '皮具箱包' THEN 18
              |  WHEN '配饰' THEN 21
              |END AS type
            """.stripMargin, // TODO MDML-25-RESOLVED toLabel不是很直接,这种封装意义不大, 如下calBrand
            "case cate3 when '运动服饰' then '运动服装' " +
              "when '户外' then '户外服装' " +
              "when '其他运动装备' then '其它运动装备' else cate3 end as sub_type",
            "cate_count as cnt", "percent")
        )
        .union(
          priceLevelDF
            /* 品牌偏好计算, 编码为男装/女装/内衣/鞋子/运动/皮具箱包/配饰=>5/8/11/14/17/20/23 */
            .selectExpr("uuid", "'CP002' as label",
            """
              |CASE cate2
              |  WHEN '男装' THEN 5
              |  WHEN '女装' THEN 8
              |  WHEN '内衣' THEN 11
              |  WHEN '鞋子' THEN 24
              |  WHEN '运动' THEN 17
              |  WHEN '皮具箱包' THEN 20
              |  WHEN '配饰' THEN 23
              |END AS type
            """.stripMargin,
            "price", "cate_count as cnt", "percent")
        )
        .union(
          calBrand()
            /* 品牌偏好计算, 编码为男装/女装/内衣/鞋子/运动/皮具箱包/配饰=>4/7/10/13/16/19/22 */
            .selectExpr("uuid", "'CP002' as label",
            """
              |CASE cate2
              |  WHEN '男装' THEN 4
              |  WHEN '女装' THEN 7
              |  WHEN '内衣' THEN 10
              |  WHEN '男鞋' THEN 13
              |  WHEN '女鞋' THEN 14
              |  WHEN '运动' THEN 16
              |  WHEN '皮具箱包' THEN 19
              |  WHEN '配饰' THEN 22
              |END AS type
            """.stripMargin,
            "brand as sub_type", "0 as cnt", "percent")
        ) // .filter($"type".notEqual(-1))
    } else emptyDataFrame
  }

  override protected def clear(moduleName: String): Unit = {
    originalDF.unpersist()
    groupedDF.unpersist()
    joinedDF.unpersist()
    subCateDF.unpersist()
    priceLevelDF.unpersist()
    phOriginalDF.unpersist()
    phJoinedDF.unpersist()
    phGroupedDF.unpersist()
    logger.info(s"$moduleName|clear succeeded...")
  }

  /* 大类别标签计算 */
  @transient private def calMegaCate(): DataFrame = {
    joinedDF.select($"uuid", $"cate", $"cnt", $"mega_cate_cnt")
      .groupBy($"uuid", $"cate")
      .agg(sum($"mega_cate_cnt").as("mega_cate_cnt"), sum($"cnt").as("cnt"))
      .select($"uuid", $"cate", $"mega_cate_cnt", ($"mega_cate_cnt" / $"cnt").as("percent"))
  }

  /* 子类别标签计算 */
  @transient private def calSubCate(): DataFrame = {
    joinedDF.select($"uuid", $"cate2", $"cate3", $"cnt", $"cate_cnt")
      .groupBy($"uuid", $"cate2", $"cate3")
      .agg(sum($"cate_cnt").as("cate_cnt"), sum($"cnt").as("cnt"))
      .select($"uuid", $"cate2", $"cate3", $"cate_cnt", ($"cate_cnt" / $"cnt").as("percent"))
      .withColumn("percent", $"percent" / sum("percent").over(Window.partitionBy($"uuid", $"cate2")))
      .map {
        case Row(uuid: String, cate2: String, cate3: String, cateCount: Double, percent: Double) =>
          val _percent = (new Random((100 * percent).toInt).nextDouble() * 0.2 + 1.1) * percent
          (uuid, cate2, cate3, cateCount, _percent)
      }.toDF("uuid", "cate2", "cate3", "cate_count", "percent").cache()
  }

  /* 价格偏好计算 */
  @transient private def calPriceLevel(): DataFrame = {
    joinedDF.select($"uuid", $"cate2", $"price", $"cnt", $"cate_cnt")
      .groupBy($"uuid", $"cate2", $"price")
      .agg(sum($"cate_cnt").as("cate_cnt"), sum($"cnt").as("cnt"))
      .select($"uuid", $"cate2", $"price", $"cate_cnt", ($"cate_cnt" / $"cnt").as("percent"))
      .withColumn("percent", $"percent" / sum("percent").over(Window.partitionBy($"uuid", $"cate2")))
      .map {
        case Row(uuid: String, cate2: String, price: String, cateCount: Double, percent: Double) =>
          val _percent = (new Random((100 * percent).toInt).nextDouble() * 0.2 + 1.1) * percent
          (uuid, cate2, price, cateCount, _percent)
      }.toDF("uuid", "cate2", "price", "cate_count", "percent").cache()
  }

  /* 品牌偏好计算 */
  @transient private def calBrand(): DataFrame = {
    val cityLevelMappingBC = loadCityLevelMapping()
    val cityLevelCateClothingBrandMappingBC = loadCityLevelCateClothingBrandMapping()
    val (cityCateClothingBrandMappingBC, cityListBC) = loadCityCateClothingBrandMapping()
    val cityTempDF = originalDF.select("uuid", "cnt", "province_cn", "city_cn")
      .filter($"province_cn".isNotNull and trim($"province_cn").notEqual("") and trim($"province_cn").notEqual("未知")
        and $"city_cn".isNotNull and trim($"city_cn").notEqual("") and trim($"city_cn").notEqual("未知"))
      .groupBy("uuid", "province_cn", "city_cn")
      .agg(sum("cnt").as("cnt"))
      .withColumn("percent", $"cnt".cast(DoubleType) / sum("cnt").over(Window.partitionBy($"uuid")))
      .map {
        case Row(uuid: String, province_cn: String, city_cn: String, cnt: Long, percent: Double) =>
          val cityLevel: Int = cityLevelMappingBC.value.getOrElse(city_cn, 0)
          (uuid, province_cn, city_cn, cityLevel, cnt, percent)
      }.toDF("uuid", "province_cn", "city_cn", "city_level", "cnt", "percent")
    val cate2JoinedDF = priceLevelDF.join(
      subCateDF,
      priceLevelDF("uuid") === subCateDF("uuid") and priceLevelDF("cate2") === subCateDF("cate2"), "inner"
    ).drop(priceLevelDF("uuid"))
      .drop(priceLevelDF("cate2"))
      .select($"uuid", $"cate2", $"cate3", $"price",
        /* 每分类下取价格范围和子分类偏好的较小占比 */
        when(priceLevelDF("percent").gt(subCateDF("percent")), subCateDF("percent"))
          .otherwise(priceLevelDF("percent")).as("percent"))
    cityTempDF.join(cate2JoinedDF, cityTempDF("uuid") === cate2JoinedDF("uuid"), "inner")
      .drop(cityTempDF("uuid"))
      /* 结合上面的"较小占比"和城市偏好占比作为最终占比 */
      // 下面的price是中文
      .select($"uuid", $"cate3", $"price", $"province_cn", $"city_cn", $"city_level",
      cityTempDF("percent").multiply(cate2JoinedDF("percent")).as("percent"))
      .flatMap {
        case Row(uuid: String, cate3: String, price: String, provinceCN: String,
        cityCN: String, cityLevel: Int, percent: Double) =>
          if (cityListBC.value.contains(cityCN)) {
            /* 当该数据在城市列表中时, 使用城市系数表进行计算 */
            // key is (province_cn"), trim($"city"), trim($"cat3"), trim($"price_level")
//            println(s"now print cityCateClothingBrandMappingKey=>  ${ concat4(provinceCN, cityCN, cate3, price)} " +
//              s" cityCateClothingBrandMappingBC => ${cityCateClothingBrandMappingBC.value} ")
            val priceCode = price2codeBC.value.getOrElse(price, -1)
            val cityCateClothingBrandMappingKey = concat4(provinceCN, cityCN, cate3, priceCode.toString)
//            val cityCateClothingBrandMappingKey = concat4(provinceCN, cityCN, cate3, price)
            cityCateClothingBrandMappingBC.value.get(cityCateClothingBrandMappingKey) match {
              case Some(it: Iterable[(String, String, String, Double, String)]) =>
                it.map {
                  case (cat2, cat3, brand, cityPercent, _) =>
                    /* 二级分类为鞋子时, 取三级分类的数值(男鞋/女鞋) */
                    val _cate2 = if (cat2.equals("鞋子")) cat3 else cat2
//                    val priceLevelCode = price2code.getOrElse(price, -1)
                    val priceLevelCode = price2codeBC.value.getOrElse(price, -1)
                    (uuid, _cate2, brand, percent * cityPercent, priceLevelCode)
                }
              case None =>
                Seq.empty[(String, String, String, Double, Int)]
            }
          } else {
            /* 当该数据不在城市列表中时, 使用城市等级系数表进行计算 */
            val cityLevelCateClothingBrandMappingKey = concat4(provinceCN, cityLevel.toString, cate3, price)
            // // key is concat(trim($"province_cn"), trim($"level".cast(StringType)), trim($"cat3"),
            //      //        trim($"price_level"))
//            println(s"now print cityLevelCateClothingBrandMappingKey=> " +
//              s" ${ concat4(provinceCN, cityLevel.toString, cate3, price)} " +
//              s" cityLevelCateClothingBrandMappingBC => ${cityLevelCateClothingBrandMappingBC.value} ")
            cityLevelCateClothingBrandMappingBC.value.get(cityLevelCateClothingBrandMappingKey) match {
              case Some(it: Iterable[(String, String, String, Double)]) =>
                it.map {
                  case (cat2, cat3, brand, cityPercent) =>
                    val _cate2 = if (cat2.equals("鞋子")) cat3 else cat2
                    val priceLevelCode = price2code.getOrElse(price, -1)
                    (uuid, _cate2, brand, percent * cityPercent, priceLevelCode)
                }
              case None =>
                Seq.empty[(String, String, String, Double, Int)]
              // null: Seq[(String, String, String, Double, Int)]
            }
          }
      }
      .toDF("uuid", "cate2", "brand", "percent", "price")
      .groupBy("uuid", "cate2", "brand", "price")
      .agg(sum("percent").as("percent"))
  }

  /* 购买习惯偏好计算准备-用户设备app列表 */
  @transient private lazy val phOriginalDF: DataFrame = {
    if (needCal) {
      sourceDF.select("uuid", "device", "applist")
        .filter($"applist".isNotNull and trim($"applist").notEqual("")).cache()
    } else emptyDataFrame
  }
  /* 购买习惯偏好计算准备-用户app指定分类聚合计数 */
  @transient private lazy val phJoinedDF: DataFrame = {
    if (needCal) {
      val appCategoryMappingBC = loadAppCategoryMapping()
      phOriginalDF.flatMap {
        case Row(uuid: String, device: String, applist: String) =>
          applist.split(",").map {
            apppkg => appCategoryMappingBC.value.getOrElse(apppkg, "")
          }.filter(_.nonEmpty).map {
            catel2 => (uuid, device, catel2)
          }
      }.toDF("uuid", "device", "cate_l2").distinct().cache()
    } else emptyDataFrame
  }
  /* 购买习惯偏好计算准备-用户app指定分类偏好计算 */
  @transient private lazy val phGroupedDF: DataFrame = {
    if (needCal) {
      val uuid2count: Broadcast[Map[String, Long]] = broadcast(
        phJoinedDF.groupBy($"uuid").agg(count("device").as("cnt"))
          .select("uuid", "cnt")
          .collect().map {
          case Row(uuid: String, cnt: Long) =>
            uuid -> cnt
        }.toMap
      )

      phJoinedDF.groupBy("uuid", "cate_l2")
        .agg(count("device").as("cnt"))
        .select("uuid", "cate_l2", "cnt")
        .map {
          case Row(uuid: String, catel2: String, cnt: Long) =>
            val totalCount = if (uuid2count.value.getOrElse(uuid, 1L) == 0) 1 else uuid2count.value.getOrElse(uuid, 1L)
            (uuid, appCate2textBC.value(catel2), cnt, totalCount, cnt.toDouble / totalCount)
        }.toDF("uuid", "label_id", "cnt", "total", "percent")
        .cache()
    } else emptyDataFrame
  }

  /* 计算购物偏好, 根据在装应用分类占比体现 */
  private def calPurchaseHabit(): DataFrame = {
    /* 计算线下购物偏好, 根据线上购物偏好最高的分类进行换算, 不同范围乘以不同的随机数 */
    phGroupedDF.select("uuid", "percent")
      .groupBy("uuid")
      /* 取各用户线上标签的最大占比 */
      .agg(max("percent").as("percent"))
      .map {
        case Row(uuid: String, percent: Double) =>
          val random = new Random((percent * 100).toInt)
          /* 按照最大占比数值根据不同的随机值计算线下购物偏好 */
          val _percent = percent match {
            case value: Double if value > 0.9 =>
              value * (0.8 + 0.2 * random.nextDouble())
            case value: Double if value > 0.8 && value <= 0.9 =>
              value * (1.0 + 0.1 * random.nextDouble())
            case value: Double if value > 0.7 && value <= 0.8 =>
              value * (1.1 + 0.1 * random.nextDouble())
            case value: Double if value > 0.6 && value <= 0.7 =>
              value * (1.2 + 0.2 * random.nextDouble())
            case value: Double if value > 0.5 && value <= 0.6 =>
              value * (1.4 + 0.2 * random.nextDouble())
            case value: Double if value <= 0.5 =>
              value * (1.6 + 0.2 * random.nextDouble())
            case _ => 0.0
          }
          (uuid, "关注线下实体店", 0, _percent)
      }.toDF("uuid", "label_id", "cnt", "percent")
      .union(phGroupedDF.select("uuid", "label_id", "cnt", "percent"))
      .select($"uuid", lit(labelName).as("label"), lit(2).as("type"), $"label_id".as("sub_type"), $"cnt", $"percent")
  }


  /* apppkg,catel2映射字典准备 */
  def loadAppCategoryMapping(): Broadcast[Map[String, String]] = {
    /* 获取指定app类别范围的包名/类别映射关系 */
    val appCategoryMappingSQL = s"select cate_l2,apppkg from " +
      s"${PropUtils.HIVE_TABLE_APP_CATEGORY_MAPPING_PAR} where version='1000' and cate_l2 in " +
      s"('在线购物', '团购优惠券', '跨境电商', '品牌电商', '导购', '生鲜电商', '购物分享')"
    logger.info(appCategoryMappingSQL)
    val app2cat2 = sql(appCategoryMappingSQL)
      .map { case Row(cat2: String, appPkg: String) => appPkg -> cat2 }.collect().toMap
    logger.info("app2cat2 " + app2cat2.take(20))
    broadcast(
      app2cat2
    )
  }

  /* 加载服装大分类映射表 */
  def loadClothingMegaCateMapping(): Broadcast[Map[Int, Iterable[(String, Double)]]] = {
    val clothingMegaCateMapping = table(PropUtils.HIVE_TABLE_CLOTHING_MEGA_CATE_MAPPING)
      .select($"agebin", $"gender", $"occupation".as("industry"), $"cate", $"weight")
      .rdd.map {
      case Row(agebin: Int, gender: Int, industry: Int, cate: String, weight: Double) =>
        /* 将agebin/gender/industry等字段拼接为key */
        zip3(agebin, gender, industry) -> (cate, weight)
    }.groupByKey().collectAsMap()
    logger.info("clothingMegaCateMapping " + clothingMegaCateMapping.take(20))
    broadcast(
      clothingMegaCateMapping
    )
  }

  /* 加载服装小分类价格等级映射表 */
  def loadClothingCateMapping(): Broadcast[Map[Int, Iterable[(String, String, String, Double)]]] = {
    val clothingCateMapping = table(PropUtils.HIVE_TABLE_CLOTHING_CATE_MAPPING)
      .filter($"price".isNotNull and trim($"price").notEqual(""))
      .select(
        $"agebin",
        $"gender",
        $"occupation".as("industry"),
        $"income",
        $"cate2",
        $"cate3",
        $"price",
        $"weight"
      ).rdd.map {
      case Row(
      agebin: Int, gender: Int, industry: Int,
      income: Int, cate2: String, cate3: String,
      price: String, weight: Double
      ) =>
        /* 将agebin/gender/industry/income等字段拼接为key */
        zip4(agebin, gender, industry, income) -> (cate2, cate3, price, weight)
    }.groupByKey().collectAsMap()
    logger.info("clothingCateMapping " + clothingCateMapping.take(20))
    broadcast(
      clothingCateMapping
    )
  }

  /* 加载城市名称和城市等级映射表 */
  def loadCityLevelMapping(): Broadcast[Map[String, Int]] = {
    val cityLevelCateMapping = table(PropUtils.HIVE_TABLE_CITY_LEVEL_MAPPING)
      .select($"city", $"level")
      .rdd.map {
      case Row(city: String, level: Int) =>
        city -> level
    }.collectAsMap()
    logger.info("cityLevelCateMapping" + cityLevelCateMapping.take(20))
    broadcast(
      cityLevelCateMapping
    )
  }

  /* 加载城市等级和服装品牌映射表 */
  def loadCityLevelCateClothingBrandMapping(): Broadcast[Map[String, Iterable[(String, String, String, Double)]]] = {
    val cityLevelCateClothingBrandMapping = table(PropUtils.HIVE_TABLE_CITY_LEVEL_CATE_CLOTHING_BRAND_MAPPING)
      .filter($"price_level".isNotNull and trim($"price_level").notEqual(""))
      .select(concat(trim($"province_cn"), trim($"level".cast(StringType)), trim($"cat3"),
        trim($"price_level")).as("key"), $"nameb".as("brand"), $"percent", $"cat2", $"cat3")
      .rdd.map {
      case Row(key: String, brand: String, percent: Double, cat2: String, cat3: String) =>
        key -> (cat2, cat3, brand, percent)
        // key is concat(trim($"province_cn"), trim($"level".cast(StringType)), trim($"cat3"),
      //        trim($"price_level"))
    }.groupByKey().collectAsMap()
    logger.info("cityLevelCateClothingBrandMapping " + cityLevelCateClothingBrandMapping.take(20))
    broadcast(
      cityLevelCateClothingBrandMapping
    )
  }

  /* 加载城市详情和服装品牌映射表 */
  def loadCityCateClothingBrandMapping():
  (Broadcast[Map[String, Iterable[(String, String, String, Double, String)]]], Broadcast[Set[String]]) = {
    val cityCateClothingBrandMapping = table(PropUtils.HIVE_TABLE_CITY_CATE_CLOTHING_BRAND_MAPPING)
      .filter($"price_level".isNotNull and trim($"price_level").notEqual(""))
      /* 拼接province_cn/city/cat3/price_level等字段作为键 */
      .select(concat(trim($"province_cn"), trim($"city"), trim($"cat3"), trim($"price_level")).as("key"),
      $"nameb".as("brand"), $"percent", $"cat2", $"cat3", $"city")
      .rdd.map {
      case Row(key: String, brand: String, percent: Double, cat2: String, cat3: String, city: String) =>
        key -> (cat2, cat3, brand, percent, city)
        // key is (province_cn"), trim($"city"), trim($"cat3"), trim($"price_level")
    }.groupByKey().collectAsMap()
    logger.info("cityCateClothingBrandMapping " + cityCateClothingBrandMapping.take(20))
    /* 把城市名称列表单独取出, 用于判断源数据的计算逻辑 */
    val cityList = cityCateClothingBrandMapping.flatMap(_._2.map(_._5)).toSet
    logger.info("cityList " + cityList.take(20))
    broadcast(cityCateClothingBrandMapping) -> broadcast(cityList)
  }

  /** 由agebin, gender, industry组成的主键, 共8bit
   * 其中agebin=[5,9], gender=[0,1], industry=[0,12]
   */
  private def zip3(agebin: Int, gender: Int, industry: Int): Int = {
    (industry << 4) + ((agebin - 5) << 1) + gender
  }

  /** 由agebin, gender, industry, income组成的主键, 共10bit
   * 其中agebin=[5,9], gender=[0,1], industry=[0,12], income=[0,2]
   */
  private def zip4(agebin: Int, gender: Int, industry: Int, income: Int): Int = {
    (income << 8) + (industry << 4) + ((agebin - 5) << 1) + gender
  }

  /** 由agebin, gender, industry, income组成的主键, 共10bit
   * 其中agebin=[5,9], gender=[0,1], industry=[0,12], income=[0,2]
   */
  private def zip4(income: Int, zip3Key: Int): Int = {
    (income << 8) + zip3Key
  }

  /* 字符串拼接 */
  private def concat4(strs: String*): String = {
    strs.map(_.trim).foldLeft(new StringBuilder(""))((sb: mutable.StringBuilder, str) => sb.append(str)).toString()
  }

  /* 根据字段名称返回与之对应的标签名称 */
  private def toLabel(s: String, index: Int): Int = cate2code.get(s) match {
    case Some(res) if index == 1 => res._1
    case Some(res) if index == 2 => res._2
    case Some(res) if index == 3 => res._3
    case None => -1
  }

  /* app二级分类和购买习惯映射 */
//  val appCate2text = Map(
//    "在线购物" -> "关注综合购物网站",
//    "团购优惠券" -> "关注折扣信息",
//    "跨境电商" -> "关注跨境海淘",
//    "品牌电商" -> "关注品牌电商",
//    "导购" -> "关注导购信息",
//    "生鲜电商" -> "关注综合购物网站",
//    "购物分享" -> "关注购物分享"
//  )
  /* 价格范围和编码映射 */
//  private val price2code = Map(
//    "低档" -> 1,
//    "平价" -> 2,
//    "中档" -> 3,
//    "轻奢" -> 4,
//    "奢侈" -> 5
//  )
  /* 子类别和编码映射 */
  private val cate2code = Map(
    "男装" -> (3, 4, 5),
    "女装" -> (6, 7, 8),
    "内衣" -> (9, 10, 11),
    "鞋子" -> (12, 13, 24),
    "运动" -> (15, 16, 17),
    "皮具箱包" -> (18, 19, 20),
    "配饰" -> (21, 22, 23)
  )
}
