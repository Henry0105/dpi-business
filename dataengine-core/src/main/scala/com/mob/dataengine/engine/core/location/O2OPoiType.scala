package com.mob.dataengine.engine.core.location

import com.mob.dataengine.engine.core.location.enu.OutputOperationTypeEnu
import com.mob.dataengine.engine.core.location.enu.OutputOperationTypeEnu.OutputOperationTypeEnu
import com.mob.dataengine.engine.core.location.util.GeoHashUDF
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * #Owner: zhangnw
 * #describe: 经纬度距离计算的工具类
 * #projectName:
 * #BusinessName:
 * #SourceTable:
 * #TargetTable:
 * #CreateDate: 2017/9/19 14:31
 */
object O2OPoiType extends Serializable {

  private val instance = new O2OPoiType

  private[this] val logger: Logger = Logger.getLogger(O2OPoiType.getClass)

  def apply: O2OPoiType = instance

  /**
   * 找LBS中经纬度点周围的POI点 O2O项目中使用的方法
   * 结果数据会与原始的LBS数据join，没有匹配到的数据也保留
   *
   * @param spark               spark
   * @param inputPoiSql         输入的POI数据
   * @param inputLbsSql         输入的LBS数据
   * @param useMapJoin          是否使用MapJoin 建议LBS数据量较小的时候设为true
   * @param repartitionCount    分区数量
   * @param outputOperationType 输出操作类型
   * @param outputTableName     输出表名
   * @param distanceValue       距离阈值
   */
  def lbsPoiJoin(spark: SparkSession, inputPoiSql: String, inputLbsSql: String,
                 useMapJoin: Boolean, repartitionCount: Int,
                 outputOperationType: OutputOperationTypeEnu,
                 outputTableName: String, distanceValue: Double): Unit =
    instance.lbsPoiJoin(logger, spark, inputPoiSql, inputLbsSql,
      useMapJoin, repartitionCount,
      outputOperationType, outputTableName, distanceValue)


  /**
   * 找LBS中经纬度点周围的POI点 O2O项目中使用的方法
   * 结果数据不会与原始的LBS数据join，没有匹配到的数据不会保留下来
   *
   * @param spark               spark
   * @param inputPoiSql         输入的POI数据
   * @param inputLbsSql         输入的LBS数据
   * @param useMapJoin          是否使用MapJoin 建议LBS数据量较小的时候设为true
   * @param repartitionCount    分区数量
   * @param outputOperationType 输出操作类型
   * @param outputTableName     输出表名
   * @param distanceValue       距离阈值
   */
  def lbsPoiJoinNotNull(spark: SparkSession, inputPoiSql: String, inputLbsSql: String,
                        useMapJoin: Boolean, repartitionCount: Int,
                        outputOperationType: OutputOperationTypeEnu,
                        outputTableName: String, distanceValue: Double): Unit =
    instance.lbsPoiJoinNotNull(logger, spark, inputPoiSql, inputLbsSql,
      useMapJoin, repartitionCount,
      outputOperationType, outputTableName, distanceValue)


  /**
   * 找LBS中经纬度点周围的POI点 O2O项目中使用的方法
   * 结果数据不会与原始的LBS数据join，没有匹配到的数据不会保留下来
   * 输出的时候多输出一个type字段
   *
   * @param spark               spark
   * @param inputPoiSql         输入的POI数据
   * @param inputLbsSql         输入的LBS数据
   * @param useMapJoin          是否使用MapJoin 建议LBS数据量较小的时候设为true
   * @param repartitionCount    分区数量
   * @param outputOperationType 输出操作类型
   * @param outputTableName     输出表名
   * @param distanceValue       距离阈值
   */
  def lbsPoiJoinNewNotNull(spark: SparkSession, inputPoiSql: String, inputLbsSql: String,
                           useMapJoin: Boolean, repartitionCount: Int,
                           outputOperationType: OutputOperationTypeEnu,
                           outputTableName: String, distanceValue: Double): Unit =
    instance.lbsPoiJoinNewNotNull(logger, spark, inputPoiSql, inputLbsSql,
      useMapJoin, repartitionCount,
      outputOperationType, outputTableName, distanceValue)

}

class O2OPoiType private extends Serializable {


  def lbsPoiJoinNotNull(logger: Logger, spark: SparkSession, inputPoiSql: String, inputLbsSql: String,
                        useMapJoin: Boolean, repartitionCount: Int,
                        outputOperationType: OutputOperationTypeEnu,
                        outputTableName: String, distanceValue: Double): Unit = {

    // 输出参数
    logger.info(
      s"""
         |
         |lbs poi join --> get lat lon distance
         |--------------------------------------------------
         |inputPoiSql:
         |$inputPoiSql
         |inputLbsSql:
         |$inputLbsSql
         |useMapJoin:
         |$useMapJoin
         |repartitionCount:
         |$repartitionCount
         |outputOperationType:
         |$outputOperationType
         |outputTableName:
         |$outputTableName
         |distanceValue:
         |$distanceValue
         |--------------------------------------------------
         |
        """.stripMargin)

    import spark.implicits._

    val lbsTbNameArr = Array("lbs_0", "lbs_1", "lbs_2", "lbs_3", "lbs_4", "lbs_5", "lbs_6", "lbs_7", "lbs_8")
    val mapJoinStr = if (useMapJoin) " /*+MAPJOIN(A)*/ " else ""

    val joinSqlArrayBuffer = new ArrayBuffer[String]()
    for (lbsTbNameItem <- lbsTbNameArr) {
      val joinSql =
        s"""
           | SELECT
           | $mapJoinStr
           | A.lat loc_lat,A.lon loc_lon,B.lat poi_lat, B.lon poi_lon, B.name,B.type_id
           | FROM $lbsTbNameItem A
           | INNER JOIN poi B
           | on A.g7 = B.g7
           | WHERE B.lat IS NOT NULL AND B.lon IS NOT NULL
        """.stripMargin

      logger.info(
        s"""
           |
           |--------------------------------------------------
           |join SQL:
           |$joinSql
           |--------------------------------------------------
           |
        """.stripMargin)
      joinSqlArrayBuffer += joinSql
    }

    // 读取输入数据
    // poi表
    val poiInDF = spark.sql(inputPoiSql)
    poiInDF.createOrReplaceTempView("poi")
    // LBS表
    val locationInDF = spark.sql(inputLbsSql)
    locationInDF.createOrReplaceTempView("input_lbs")

    // 使用UDF获取GeoHash所有9个格子对应的字符串
    val schema: StructType = StructType(
      StructField("around_0", StringType, nullable = false) ::
        StructField("around_1", StringType, nullable = false) ::
        StructField("around_2", StringType, nullable = false) ::
        StructField("around_3", StringType, nullable = false) ::
        StructField("around_4", StringType, nullable = false) ::
        StructField("around_5", StringType, nullable = false) ::
        StructField("around_6", StringType, nullable = false) ::
        StructField("around_7", StringType, nullable = false) ::
        StructField("around_8", StringType, nullable = false) ::
        Nil
    )
    // 注册UDF 获取周边GeoHash
    spark.udf.register("around", GeoHashUDF.geoHashAroundByLvUDF, schema)
    spark.sql("SELECT lat,lon,around(7,lat,lon) around FROM input_lbs")
      .createOrReplaceTempView("lbs")

    // LBS表的数据分为9份，每个格子一份
    spark.sql("SELECT lat,lon,around.around_0 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(0))
    spark.sql("SELECT lat,lon,around.around_1 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(1))
    spark.sql("SELECT lat,lon,around.around_2 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(2))
    spark.sql("SELECT lat,lon,around.around_3 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(3))
    spark.sql("SELECT lat,lon,around.around_4 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(4))
    spark.sql("SELECT lat,lon,around.around_5 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(5))
    spark.sql("SELECT lat,lon,around.around_6 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(6))
    spark.sql("SELECT lat,lon,around.around_7 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(7))
    spark.sql("SELECT lat,lon,around.around_8 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(8))

    // 每个格子的LBS数据分别和POI表进行JOIN
    val joined_0 = spark.sql(joinSqlArrayBuffer(0)).repartition(repartitionCount)
    val joined_1 = spark.sql(joinSqlArrayBuffer(1)).repartition(repartitionCount)
    val joined_2 = spark.sql(joinSqlArrayBuffer(2)).repartition(repartitionCount)
    val joined_3 = spark.sql(joinSqlArrayBuffer(3)).repartition(repartitionCount)
    val joined_4 = spark.sql(joinSqlArrayBuffer(4)).repartition(repartitionCount)
    val joined_5 = spark.sql(joinSqlArrayBuffer(5)).repartition(repartitionCount)
    val joined_6 = spark.sql(joinSqlArrayBuffer(6)).repartition(repartitionCount)
    val joined_7 = spark.sql(joinSqlArrayBuffer(7)).repartition(repartitionCount)
    val joined_8 = spark.sql(joinSqlArrayBuffer(8)).repartition(repartitionCount)

    // UNION 9个join的结果
    joined_0.union(joined_1).union(joined_2).union(joined_3)
      .union(joined_4).union(joined_5).union(joined_6)
      .union(joined_7).union(joined_8)
      .createOrReplaceTempView("joined_df")

    // 注册UDF 计算距离
    spark.udf.register("distance", GeoHashUDF.distanceUDF, DoubleType)
    spark.sql(
      """
        |SELECT
        |loc_lat,loc_lon,poi_lat,poi_lon,name,type_id,
        |distance(loc_lat,loc_lon,poi_lat,poi_lon) distance
        |FROM joined_df
      """.stripMargin)
      .where($"distance" < distanceValue)
      .createOrReplaceTempView("joined_dis_ok")

    // 注册UDF 合并字符串作为后续操作的键
    // lbs纬度/lbs经度/类型联合作为Key,找出距离最小的POI点
    spark.udf.register("lat_lon_type", GeoHashUDF.lat_lon_typeId_key, StringType)
    // lbs纬度/lbs经度作为Key,合并所有类型的POI点
    spark.udf.register("lat_lon", GeoHashUDF.lat_lon_key, StringType)

    // 获取联合的keys
    val withKeyDF = spark.sql(
      """
        |SELECT
        |loc_lat,loc_lon,poi_lat,poi_lon,
        |name,type_id, distance,
        |lat_lon_type(loc_lat,loc_lon,type_id) key3,
        |lat_lon(loc_lat,loc_lon) key2
        |FROM joined_dis_ok
      """.stripMargin)

    // 转换成RDD 转换Key 按poi类型分类保留距离最小的一个poi地址
    val typeSortRdd =
      withKeyDF.rdd
        .map(row => (row.getString(7), row))
        .reduceByKey(
          (r1: Row, r2: Row) => {
            if (r1.getDouble(6) < r2.getDouble(6)) {
              r1
            }
            else if (r1.getDouble(6) > r2.getDouble(6)) {
              r2
            }
            else {
              if (r1.getString(4) > r2.getString(4)) r1 else r2
            }
          })

    // 以LBS的经纬度作为Key分组 整合每种类型的Poi地址的维度/经度/地址名称
    val outRDD =
      typeSortRdd
        .map(row => (row._2.getString(8), row._2))
        .groupByKey()
        .map(t2 => {
          var loc_lat = ""
          var loc_lon = ""
          var poi_lat = ""
          var poi_lon = ""
          val typeArr = new Array[Row](10)
          for (item <- t2._2) {
            loc_lat = item.getString(0)
            loc_lon = item.getString(1)
            // 小数点后保留6位
            poi_lat = item.getString(2).toDouble.formatted("%.6f").toString
            poi_lon = item.getString(3).toDouble.formatted("%.6f").toString
            val poi_name = item.getString(4)
            val type_id = item.getString(5).toInt
            type_id match {
              case 1 => typeArr(0) = Row(poi_lat, poi_lon, poi_name)
              case 2 => typeArr(1) = Row(poi_lat, poi_lon, poi_name)
              case 3 => typeArr(2) = Row(poi_lat, poi_lon, poi_name)
              case 4 => typeArr(3) = Row(poi_lat, poi_lon, poi_name)
              case 5 => typeArr(4) = Row(poi_lat, poi_lon, poi_name)
              case 6 => typeArr(5) = Row(poi_lat, poi_lon, poi_name)
              case 7 => typeArr(6) = Row(poi_lat, poi_lon, poi_name)
              case 8 => typeArr(7) = Row(poi_lat, poi_lon, poi_name)
              case 9 => typeArr(8) = Row(poi_lat, poi_lon, poi_name)
              case 10 => typeArr(9) = Row(poi_lat, poi_lon, poi_name)
              case _ =>
            }
          }

          val type_1 = if (typeArr(0) != null) typeArr(0) else Row("", "", "")
          val type_2 = if (typeArr(1) != null) typeArr(1) else Row("", "", "")
          val type_3 = if (typeArr(2) != null) typeArr(2) else Row("", "", "")
          val type_4 = if (typeArr(3) != null) typeArr(3) else Row("", "", "")
          val type_5 = if (typeArr(4) != null) typeArr(4) else Row("", "", "")
          val type_6 = if (typeArr(5) != null) typeArr(5) else Row("", "", "")
          val type_7 = if (typeArr(6) != null) typeArr(6) else Row("", "", "")
          val type_8 = if (typeArr(7) != null) typeArr(7) else Row("", "", "")
          val type_9 = if (typeArr(8) != null) typeArr(8) else Row("", "", "")
          val type_10 = if (typeArr(9) != null) typeArr(9) else Row("", "", "")

          Row(loc_lat, loc_lon,
            type_1, type_2, type_3, type_4, type_5,
            type_6, type_7, type_8, type_9, type_10
          )
        })


    // 定义输出表的Schema
    val outSchema = StructType(
      StructField("lat", StringType, nullable = false) ::
        StructField("lon", StringType, nullable = false) ::
        StructField("type_1", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_2", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_3", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_4", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_5", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_6", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_7", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_8", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_9", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_10", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true)
        :: Nil
    )

    val outDbName = outputTableName.split("\\.")(0)
    val outTbName = outputTableName.split("\\.")(1)
    // 选择输出的数据库
    spark.sql("USE " + outDbName)
    // 创建输出的DF
    val outDF = spark.createDataFrame(outRDD, outSchema)

    outDF.createOrReplaceTempView("out_join_lbs")

    outputOperationType match {

      case OutputOperationTypeEnu.save2HiveTable =>
        outDF.write.mode(SaveMode.Overwrite).saveAsTable(outTbName)

      case OutputOperationTypeEnu.cacheTable =>

        spark.sql(

          s"""
             |CACHE TABLE $outputTableName AS
             |SELECT * FROM out_join_lbs
              """.stripMargin

        )

      case _ => println("no available output operation")
    }


  }

  def lbsPoiJoin(logger: Logger, spark: SparkSession, inputPoiSql: String, inputLbsSql: String,
                 useMapJoin: Boolean, repartitionCount: Int,
                 outputOperationType: OutputOperationTypeEnu,
                 outputTableName: String, distanceValue: Double): Unit = {

    // 输出参数
    logger.info(
      s"""
         |
         |lbs poi join --> get lat lon distance test o2o o2o
         |--------------------------------------------------
         |inputPoiSql:
         |$inputPoiSql
         |inputLbsSql:
         |$inputLbsSql
         |useMapJoin:
         |$useMapJoin
         |repartitionCount:
         |$repartitionCount
         |outputOperationType:
         |$outputOperationType
         |outputTableName:
         |$outputTableName
         |distanceValue:
         |$distanceValue
         |--------------------------------------------------
         |
        """.stripMargin)

    import spark.implicits._

    val lbsTbNameArr = Array("lbs_0", "lbs_1", "lbs_2", "lbs_3", "lbs_4", "lbs_5", "lbs_6", "lbs_7", "lbs_8")
    val mapJoinStr = if (useMapJoin) " /*+MAPJOIN(A)*/ " else ""

    val joinSqlArrayBuffer = new ArrayBuffer[String]()
    for (lbsTbNameItem <- lbsTbNameArr) {
      val joinSql =
        s"""
           | SELECT
           | $mapJoinStr
           | A.lat loc_lat,A.lon loc_lon,B.lat poi_lat, B.lon poi_lon, B.name,B.type_id
           | FROM $lbsTbNameItem A
           | INNER JOIN poi B
           | on A.g7 = B.g7
           | WHERE B.lat IS NOT NULL AND B.lon IS NOT NULL
        """.stripMargin

      logger.info(
        s"""
           |
           |--------------------------------------------------
           |join SQL:
           |$joinSql
           |--------------------------------------------------
           |
        """.stripMargin)
      joinSqlArrayBuffer += joinSql
    }

    // 读取输入数据
    // poi表
    val poiInDF = spark.sql(inputPoiSql)
    poiInDF.createOrReplaceTempView("poi")
    // LBS表
    val locationInDF = spark.sql(inputLbsSql)
    locationInDF.createOrReplaceTempView("input_lbs")

    // 使用UDF获取GeoHash所有9个格子对应的字符串
    val schema: StructType = StructType(
      StructField("around_0", StringType, nullable = false) ::
        StructField("around_1", StringType, nullable = false) ::
        StructField("around_2", StringType, nullable = false) ::
        StructField("around_3", StringType, nullable = false) ::
        StructField("around_4", StringType, nullable = false) ::
        StructField("around_5", StringType, nullable = false) ::
        StructField("around_6", StringType, nullable = false) ::
        StructField("around_7", StringType, nullable = false) ::
        StructField("around_8", StringType, nullable = false) ::
        Nil
    )
    // 注册UDF 获取周边GeoHash
    spark.udf.register("around", GeoHashUDF.geoHashAroundByLvUDF, schema)
    spark.sql("SELECT lat,lon,around(7,lat,lon) around FROM input_lbs")
      .createOrReplaceTempView("lbs")

    // LBS表的数据分为9份，每个格子一份
    spark.sql("SELECT lat,lon,around.around_0 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(0))
    spark.sql("SELECT lat,lon,around.around_1 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(1))
    spark.sql("SELECT lat,lon,around.around_2 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(2))
    spark.sql("SELECT lat,lon,around.around_3 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(3))
    spark.sql("SELECT lat,lon,around.around_4 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(4))
    spark.sql("SELECT lat,lon,around.around_5 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(5))
    spark.sql("SELECT lat,lon,around.around_6 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(6))
    spark.sql("SELECT lat,lon,around.around_7 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(7))
    spark.sql("SELECT lat,lon,around.around_8 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(8))

    // 每个格子的LBS数据分别和POI表进行JOIN
    val joined_0 = spark.sql(joinSqlArrayBuffer(0)).repartition(repartitionCount)
    val joined_1 = spark.sql(joinSqlArrayBuffer(1)).repartition(repartitionCount)
    val joined_2 = spark.sql(joinSqlArrayBuffer(2)).repartition(repartitionCount)
    val joined_3 = spark.sql(joinSqlArrayBuffer(3)).repartition(repartitionCount)
    val joined_4 = spark.sql(joinSqlArrayBuffer(4)).repartition(repartitionCount)
    val joined_5 = spark.sql(joinSqlArrayBuffer(5)).repartition(repartitionCount)
    val joined_6 = spark.sql(joinSqlArrayBuffer(6)).repartition(repartitionCount)
    val joined_7 = spark.sql(joinSqlArrayBuffer(7)).repartition(repartitionCount)
    val joined_8 = spark.sql(joinSqlArrayBuffer(8)).repartition(repartitionCount)

    // UNION 9个join的结果
    joined_0.union(joined_1).union(joined_2).union(joined_3)
      .union(joined_4).union(joined_5).union(joined_6)
      .union(joined_7).union(joined_8)
      .createOrReplaceTempView("joined_df")

    // 注册UDF 计算距离
    spark.udf.register("distance", GeoHashUDF.distanceUDF, DoubleType)
    spark.sql(
      """
        |SELECT
        |loc_lat,loc_lon,poi_lat,poi_lon,name,type_id,
        |distance(loc_lat,loc_lon,poi_lat,poi_lon) distance
        |FROM joined_df
      """.stripMargin)
      .where($"distance" < distanceValue)
      .createOrReplaceTempView("joined_dis_ok")

    // 注册UDF 合并字符串作为后续操作的键
    // lbs纬度/lbs经度/类型联合作为Key,找出距离最小的POI点
    spark.udf.register("lat_lon_type", GeoHashUDF.lat_lon_typeId_key, StringType)
    // lbs纬度/lbs经度作为Key,合并所有类型的POI点
    spark.udf.register("lat_lon", GeoHashUDF.lat_lon_key, StringType)

    // 获取联合的keys
    val withKeyDF = spark.sql(
      """
        |SELECT
        |loc_lat,loc_lon,poi_lat,poi_lon,
        |name,type_id, distance,
        |lat_lon_type(loc_lat,loc_lon,type_id) key3,
        |lat_lon(loc_lat,loc_lon) key2
        |FROM joined_dis_ok
      """.stripMargin)

    // 转换成RDD 转换Key 按poi类型分类保留距离最小的一个poi地址
    val typeSortRdd =
      withKeyDF.rdd
        .map(row => (row.getString(7), row))
        .reduceByKey(
          (r1: Row, r2: Row) => {
            if (r1.getDouble(6) < r2.getDouble(6)) {
              r1
            }
            else if (r1.getDouble(6) > r2.getDouble(6)) {
              r2
            }
            else {
              if (r1.getString(4) > r2.getString(4)) r1 else r2
            }
          })

    // 以LBS的经纬度作为Key分组 整合每种类型的Poi地址的维度/经度/地址名称
    val outRDD =
      typeSortRdd
        .map(row => (row._2.getString(8), row._2))
        .groupByKey()
        .map(t2 => {
          var loc_lat = ""
          var loc_lon = ""
          var poi_lat = ""
          var poi_lon = ""
          val typeArr = new Array[Row](10)
          for (item <- t2._2) {
            loc_lat = item.getString(0)
            loc_lon = item.getString(1)
            // 小数点后保留6位
            poi_lat = item.getString(2).toDouble.formatted("%.6f").toString
            poi_lon = item.getString(3).toDouble.formatted("%.6f").toString
            val poi_name = item.getString(4)
            val type_id = item.getString(5).toInt
            type_id match {
              case 1 => typeArr(0) = Row(poi_lat, poi_lon, poi_name)
              case 2 => typeArr(1) = Row(poi_lat, poi_lon, poi_name)
              case 3 => typeArr(2) = Row(poi_lat, poi_lon, poi_name)
              case 4 => typeArr(3) = Row(poi_lat, poi_lon, poi_name)
              case 5 => typeArr(4) = Row(poi_lat, poi_lon, poi_name)
              case 6 => typeArr(5) = Row(poi_lat, poi_lon, poi_name)
              case 7 => typeArr(6) = Row(poi_lat, poi_lon, poi_name)
              case 8 => typeArr(7) = Row(poi_lat, poi_lon, poi_name)
              case 9 => typeArr(8) = Row(poi_lat, poi_lon, poi_name)
              case 10 => typeArr(9) = Row(poi_lat, poi_lon, poi_name)
              case _ =>
            }
          }

          val type_1 = if (typeArr(0) != null) typeArr(0) else Row("", "", "")
          val type_2 = if (typeArr(1) != null) typeArr(1) else Row("", "", "")
          val type_3 = if (typeArr(2) != null) typeArr(2) else Row("", "", "")
          val type_4 = if (typeArr(3) != null) typeArr(3) else Row("", "", "")
          val type_5 = if (typeArr(4) != null) typeArr(4) else Row("", "", "")
          val type_6 = if (typeArr(5) != null) typeArr(5) else Row("", "", "")
          val type_7 = if (typeArr(6) != null) typeArr(6) else Row("", "", "")
          val type_8 = if (typeArr(7) != null) typeArr(7) else Row("", "", "")
          val type_9 = if (typeArr(8) != null) typeArr(8) else Row("", "", "")
          val type_10 = if (typeArr(9) != null) typeArr(9) else Row("", "", "")

          Row(loc_lat, loc_lon,
            type_1, type_2, type_3, type_4, type_5,
            type_6, type_7, type_8, type_9, type_10
          )
        })


    // 定义输出表的Schema
    val outSchema = StructType(
      StructField("lat", StringType, nullable = false) ::
        StructField("lon", StringType, nullable = false) ::
        StructField("type_1", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_2", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_3", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_4", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_5", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_6", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_7", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_8", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_9", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true) ::
        StructField("type_10", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) :: Nil
        ), nullable = true)
        :: Nil
    )

    // 创建输出的DF
    spark.createDataFrame(outRDD, outSchema)
      .createOrReplaceTempView("out_join_lbs")


    // 中心点的Schema
    val centerSchema: StructType = StructType(
      StructField("lat", StringType, nullable = false) ::
        StructField("lon", StringType, nullable = false) :: Nil)
    // 注册找出中心点的UDF
    spark.udf.register("center", GeoHashUDF.geoHashCenterByLv, centerSchema)

    spark.sql(
      s"""
         |
        | SELECT
         | center(5,a.lat,a.lon) geohash_center5,
         | center(6,a.lat,a.lon) geohash_center6,
         | center(7,a.lat,a.lon) geohash_center7,
         | center(8,a.lat,a.lon) geohash_center8,
         | a.lat,a.lon,
         | b.type_1,
         | b.type_2,
         | b.type_3,
         | b.type_4,
         | b.type_5,
         | b.type_6,
         | b.type_7,
         | b.type_8,
         | b.type_9,
         | b.type_10
         | FROM input_lbs a
         | LEFT JOIN out_join_lbs b
         | ON a.lat=b.lat AND a.lon=b.lon
         |
          """.stripMargin
    )
      .createOrReplaceTempView("poi_type_tmp")

    outputOperationType match {

      case OutputOperationTypeEnu.save2HiveTable =>
        val outDF = spark.sql(
          """
            |SELECT * FROM poi_type_tmp
          """.stripMargin
        )
        val outDbName = outputTableName.split("\\.")(0)
        val outTbName = outputTableName.split("\\.")(1)
        // 选择输出的数据库
        spark.sql("USE " + outDbName)
        // 输出表
        // 如果制定format 需要提前创建空表
        // outDF.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(outTbName)
        outDF.write.mode(SaveMode.Overwrite).saveAsTable(outTbName)

      case OutputOperationTypeEnu.cacheTable =>
        spark.sql(
          s"""
             |
             | CACHE TABLE $outputTableName AS
             | SELECT * FROM poi_type_tmp
             |
          """.stripMargin)

      case _ => println("no available output operation")
    }


  }


  def lbsPoiJoinNewNotNull(logger: Logger, spark: SparkSession, inputPoiSql: String, inputLbsSql: String,
                           useMapJoin: Boolean, repartitionCount: Int,
                           outputOperationType: OutputOperationTypeEnu,
                           outputTableName: String, distanceValue: Double): Unit = {

    // 输出参数
    logger.info(
      s"""
         |
         |lbs poi join --> get lat lon distance
         |--------------------------------------------------
         |inputPoiSql:
         |$inputPoiSql
         |inputLbsSql:
         |$inputLbsSql
         |useMapJoin:
         |$useMapJoin
         |repartitionCount:
         |$repartitionCount
         |outputOperationType:
         |$outputOperationType
         |outputTableName:
         |$outputTableName
         |distanceValue:
         |$distanceValue
         |--------------------------------------------------
         |
        """.stripMargin)

    import spark.implicits._

    val lbsTbNameArr = Array("lbs_0", "lbs_1", "lbs_2", "lbs_3", "lbs_4", "lbs_5", "lbs_6", "lbs_7", "lbs_8")
    val mapJoinStr = if (useMapJoin) " /*+MAPJOIN(A)*/ " else ""

    val joinSqlArrayBuffer = new ArrayBuffer[String]()
    for (lbsTbNameItem <- lbsTbNameArr) {
      val joinSql =
        s"""
           | SELECT
           | $mapJoinStr
           | A.lat loc_lat,A.lon loc_lon,B.lat poi_lat, B.lon poi_lon, B.name,B.type,B.type_id
           | FROM $lbsTbNameItem A
           | INNER JOIN poi B
           | on A.g7 = B.g7
           | WHERE B.lat IS NOT NULL AND B.lon IS NOT NULL
        """.stripMargin

      logger.info(
        s"""
           |
           |--------------------------------------------------
           |join SQL:
           |$joinSql
           |--------------------------------------------------
           |
        """.stripMargin)
      joinSqlArrayBuffer += joinSql
    }

    // 读取输入数据
    // poi表
    val poiInDF = spark.sql(inputPoiSql)
    poiInDF.createOrReplaceTempView("poi")
    // LBS表
    val locationInDF = spark.sql(inputLbsSql)
    locationInDF.createOrReplaceTempView("input_lbs")

    // 使用UDF获取GeoHash所有9个格子对应的字符串
    val schema: StructType = StructType(
      StructField("around_0", StringType, nullable = false) ::
        StructField("around_1", StringType, nullable = false) ::
        StructField("around_2", StringType, nullable = false) ::
        StructField("around_3", StringType, nullable = false) ::
        StructField("around_4", StringType, nullable = false) ::
        StructField("around_5", StringType, nullable = false) ::
        StructField("around_6", StringType, nullable = false) ::
        StructField("around_7", StringType, nullable = false) ::
        StructField("around_8", StringType, nullable = false) ::
        Nil
    )
    // 注册UDF 获取周边GeoHash
    spark.udf.register("around", GeoHashUDF.geoHashAroundByLvUDF, schema)
    spark.sql("SELECT lat,lon,around(7,lat,lon) around FROM input_lbs")
      .createOrReplaceTempView("lbs")

    // LBS表的数据分为9份，每个格子一份
    spark.sql("SELECT lat,lon,around.around_0 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(0))
    spark.sql("SELECT lat,lon,around.around_1 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(1))
    spark.sql("SELECT lat,lon,around.around_2 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(2))
    spark.sql("SELECT lat,lon,around.around_3 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(3))
    spark.sql("SELECT lat,lon,around.around_4 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(4))
    spark.sql("SELECT lat,lon,around.around_5 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(5))
    spark.sql("SELECT lat,lon,around.around_6 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(6))
    spark.sql("SELECT lat,lon,around.around_7 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(7))
    spark.sql("SELECT lat,lon,around.around_8 g7 FROM lbs").createOrReplaceTempView(lbsTbNameArr(8))

    // 每个格子的LBS数据分别和POI表进行JOIN
    val joined_0 = spark.sql(joinSqlArrayBuffer(0)).repartition(repartitionCount)
    val joined_1 = spark.sql(joinSqlArrayBuffer(1)).repartition(repartitionCount)
    val joined_2 = spark.sql(joinSqlArrayBuffer(2)).repartition(repartitionCount)
    val joined_3 = spark.sql(joinSqlArrayBuffer(3)).repartition(repartitionCount)
    val joined_4 = spark.sql(joinSqlArrayBuffer(4)).repartition(repartitionCount)
    val joined_5 = spark.sql(joinSqlArrayBuffer(5)).repartition(repartitionCount)
    val joined_6 = spark.sql(joinSqlArrayBuffer(6)).repartition(repartitionCount)
    val joined_7 = spark.sql(joinSqlArrayBuffer(7)).repartition(repartitionCount)
    val joined_8 = spark.sql(joinSqlArrayBuffer(8)).repartition(repartitionCount)

    // UNION 9个join的结果
    joined_0.union(joined_1).union(joined_2).union(joined_3)
      .union(joined_4).union(joined_5).union(joined_6)
      .union(joined_7).union(joined_8)
      .createOrReplaceTempView("joined_df")

    // 注册UDF 计算距离
    spark.udf.register("distance", GeoHashUDF.distanceUDF, DoubleType)
    spark.sql(
      """
        |SELECT
        |loc_lat,loc_lon,poi_lat,poi_lon,name,type,type_id,
        |distance(loc_lat,loc_lon,poi_lat,poi_lon) distance
        |FROM joined_df
      """.stripMargin)
      .where($"distance" < distanceValue)
      .createOrReplaceTempView("joined_dis_ok")

    // 注册UDF 合并字符串作为后续操作的键
    // lbs纬度/lbs经度/类型联合作为Key,找出距离最小的POI点
    spark.udf.register("lat_lon_type", GeoHashUDF.lat_lon_typeId_key, StringType)
    // lbs纬度/lbs经度作为Key,合并所有类型的POI点
    spark.udf.register("lat_lon", GeoHashUDF.lat_lon_key, StringType)

    // 获取联合的keys
    val withKeyDF = spark.sql(
      """
        |SELECT
        |loc_lat,loc_lon,poi_lat,poi_lon,
        |name,type_id, distance,
        |lat_lon_type(loc_lat,loc_lon,type_id) key3,
        |lat_lon(loc_lat,loc_lon) key2,type
        |FROM joined_dis_ok
      """.stripMargin)

    // 转换成RDD 转换Key 按poi类型分类保留距离最小的一个poi地址
    val typeSortRdd =
      withKeyDF.rdd
        .map(row => (row.getString(7), row))
        .reduceByKey(
          (r1: Row, r2: Row) => {
            if (r1.getDouble(6) < r2.getDouble(6)) {
              r1
            }
            else if (r1.getDouble(6) > r2.getDouble(6)) {
              r2
            }
            else {
              if (r1.getString(4) > r2.getString(4)) r1 else r2
            }
          })

    // 以LBS的经纬度作为Key分组 整合每种类型的Poi地址的维度/经度/地址名称
    val outRDD =
      typeSortRdd
        .map(row => (row._2.getString(8), row._2))
        .groupByKey()
        .map(t2 => {
          var loc_lat = ""
          var loc_lon = ""
          var poi_lat = ""
          var poi_lon = ""
          val typeArr = new Array[Row](10)
          for (item <- t2._2) {
            loc_lat = item.getString(0)
            loc_lon = item.getString(1)
            // 小数点后保留6位
            poi_lat = item.getString(2).toDouble.formatted("%.6f").toString
            poi_lon = item.getString(3).toDouble.formatted("%.6f").toString
            val poi_name = item.getString(4)
            val type_id = item.getString(5).toInt
            val typeStr = item.getString(9)
            type_id match {
              case 1 => typeArr(0) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 2 => typeArr(1) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 3 => typeArr(2) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 4 => typeArr(3) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 5 => typeArr(4) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 6 => typeArr(5) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 7 => typeArr(6) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 8 => typeArr(7) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 9 => typeArr(8) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case 10 => typeArr(9) = Row(poi_lat, poi_lon, poi_name, typeStr)
              case _ =>
            }
          }

          val type_1 = if (typeArr(0) != null) typeArr(0) else Row("", "", "", "")
          val type_2 = if (typeArr(1) != null) typeArr(1) else Row("", "", "", "")
          val type_3 = if (typeArr(2) != null) typeArr(2) else Row("", "", "", "")
          val type_4 = if (typeArr(3) != null) typeArr(3) else Row("", "", "", "")
          val type_5 = if (typeArr(4) != null) typeArr(4) else Row("", "", "", "")
          val type_6 = if (typeArr(5) != null) typeArr(5) else Row("", "", "", "")
          val type_7 = if (typeArr(6) != null) typeArr(6) else Row("", "", "", "")
          val type_8 = if (typeArr(7) != null) typeArr(7) else Row("", "", "", "")
          val type_9 = if (typeArr(8) != null) typeArr(8) else Row("", "", "", "")
          val type_10 = if (typeArr(9) != null) typeArr(9) else Row("", "", "", "")

          Row(loc_lat, loc_lon,
            type_1, type_2, type_3, type_4, type_5,
            type_6, type_7, type_8, type_9, type_10
          )
        })

    // 定义输出表的Schema
    val outSchema = StructType(
      StructField("lat", StringType, nullable = false) ::
        StructField("lon", StringType, nullable = false) ::
        StructField("type_1", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_2", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_3", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_4", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_5", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_6", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_7", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_8", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_9", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true) ::
        StructField("type_10", StructType(
          StructField("lat", StringType, nullable = false) ::
            StructField("lon", StringType, nullable = false) ::
            StructField("name", StringType, nullable = false) ::
            StructField("type", StringType, nullable = false) ::
            Nil
        ), nullable = true)
        :: Nil
    )

    // 创建输出的DF
    spark.createDataFrame(outRDD, outSchema).createOrReplaceTempView("out")

    // 中心点的Schema
    val centerSchema: StructType = StructType(
      StructField("lat", StringType, nullable = false) ::
        StructField("lon", StringType, nullable = false) :: Nil)
    // 注册找出中心点的UDF
    spark.udf.register("center", GeoHashUDF.geoHashCenterByLv, centerSchema)

    outputOperationType match {

      case OutputOperationTypeEnu.save2HiveTable =>
        val outDF = spark.sql(
          """
            |SELECT
            |center(5,lat,lon) geohash_center5,
            |center(6,lat,lon) geohash_center6,
            |center(7,lat,lon) geohash_center7,
            |center(8,lat,lon) geohash_center8,
            |*
            |FROM out
          """.stripMargin
        )
        val outDbName = outputTableName.split("\\.")(0)
        val outTbName = outputTableName.split("\\.")(1)
        // 选择输出的数据库
        spark.sql("USE " + outDbName)
        // 输出表
        // 如果制定format 需要提前创建空表
        // outDF.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(outTbName)
        outDF.write.mode(SaveMode.Overwrite).saveAsTable(outTbName)

      case OutputOperationTypeEnu.cacheTable =>

        spark.sql(

          s"""
             |cache table $outputTableName as
             |SELECT
             |center(5,lat,lon) geohash_center5,
             |center(6,lat,lon) geohash_center6,
             |center(7,lat,lon) geohash_center7,
             |center(8,lat,lon) geohash_center8,
             |*
             |FROM out
          """.stripMargin

        )

      case _ => println("no available output operation")
    }


  }
}
