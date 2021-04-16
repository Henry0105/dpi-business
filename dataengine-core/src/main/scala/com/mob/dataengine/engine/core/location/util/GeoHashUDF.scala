package com.mob.dataengine.engine.core.location.util

import com.mob.dataengine.engine.core.location.geohash.GeoHash
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF3, UDF4}

import scala.collection.mutable.ArrayBuffer

/**
 * #Owner: zhangnw
 * #describe: GeoHash算法操作的UDF
 * #projectName:
 * #BusinessName:
 * #SourceTable:
 * #TargetTable:
 * #CreateDate: 2017/9/19 14:31
 */
object GeoHashUDF extends Serializable{

  val instance = new GeoHashUDF

  def apply: GeoHashUDF = instance

  private[this] val logger: Logger = Logger.getLogger(GeoHashUDF.getClass)


  /**
   * 计算两个经纬度距离的UDF
   *
   * @return
   */
  def distanceUDF: UDF4[String, String, String, String, Double] = new UDF4[String, String, String, String, Double] {
    override def call(t1: String, t2: String, t3: String, t4: String): Double = {
      instance.getDistance(t1.toDouble, t2.toDouble, t3.toDouble, t4.toDouble)
    }
  }

  /**
   * 经纬度--->GeoHash字符串的UDF
   * 输入GeoHash位数/维度/经度
   * 输出GeoHash对应的字符串
   * @return
   */
  def geoHashBase32ByLvUDF: UDF3[Int, String, String, String] = new UDF3[Int, String, String, String] {
    override def call(lv: Int, lat: String, lon: String): String = {
      try {
        instance.getGeoHashBase32ByLv(lv)(lat.toDouble, lon.toDouble)
      } catch {
        case ex: Exception => logger.error(ex.getMessage + "  " + lat + "/" + lon); "-1"
      }
    }
  }

  /**
   * GeoHash字符串--->经纬度
   * 输入GeoHash字符串
   * 输出经纬度
   * usage:先定义schema文件
   *    val pointSchema: StructType = StructType(
   *      StructField("lat", StringType, nullable = false) ::
   *        StructField("lon", StringType, nullable = false) :: Nil)
   * 注册UDF
   *    hiveSqlContext.udf.register("geohash2point", GeoHashUDF.getPointByGeoHashStrUDF, pointSchema)
   * @return
   */
  def getPointByGeoHashStrUDF: UDF1[String, Row] = new UDF1[String, Row] {

    override def call(t1: String): Row = {

      val lat = instance.getPointByGeoHashStr(t1)._1
      val lon = instance.getPointByGeoHashStr(t1)._2
      Row.apply(
        lat.formatted("%.6f").toString,
        lon.formatted("%.6f").toString
      )

    }

  }




  /**
   * 获取周围的8个格子
   * 输入字符等级，经纬度
   * 输出Row(0,1,2,3,4,5,6,7)
   *
   * @return
   */
  def geoHashAroundByLvUDF: UDF3[Int, String, String, Row] = new UDF3[Int, String, String, Row] {
    override def call(lv: Int, lat: String, lon: String): Row = {
      val arrayBuffer = new ArrayBuffer[String]()
      val geoHashBase32Str = instance.getGeoHashBase32ByLv(lv)(lat.toDouble, lon.toDouble)
      arrayBuffer += geoHashBase32Str
      val around = GeoHash.fromGeohashString(geoHashBase32Str).getAdjacent
      for (item <- around) {
        arrayBuffer += item.toBase32
      }
      Row.apply(arrayBuffer(0), arrayBuffer(1), arrayBuffer(2), arrayBuffer(3),
        arrayBuffer(4), arrayBuffer(5), arrayBuffer(6), arrayBuffer(7), arrayBuffer(8))
    }

    // usage:
    //    val schema: StructType = StructType(
    //      StructField("around_0", StringType, nullable = false) ::
    //        StructField("around_1", StringType, nullable = false) ::
    //        StructField("around_2", StringType, nullable = false) ::
    //        StructField("around_3", StringType, nullable = false) ::
    //        StructField("around_4", StringType, nullable = false) ::
    //        StructField("around_5", StringType, nullable = false) ::
    //        StructField("around_6", StringType, nullable = false) ::
    //        StructField("around_7", StringType, nullable = false) ::
    //        StructField("around_8", StringType, nullable = false) :: Nil)
    //    sqlContext.udf.register("around", geoHashAroundByLvUDF, schema)
  }

  /**
   * 经纬度和typeID组成联合的键
   *
   * @return
   */
  def lat_lon_typeId_key: UDF3[String, String, String, String] = new UDF3[String, String, String, String] {
    override def call(t1: String, t2: String, t3: String): String = {
      t1.replaceAll("\\.", "") + "_" + t2.replaceAll("\\.", "") + "_" + t3
    }
  }

  /**
   * 经纬度组成联合的键
   *
   * @return
   */
  def lat_lon_key: UDF2[String, String, String] = new UDF2[String, String, String] {
    override def call(t1: String, t2: String): String = {
      t1.replaceAll("\\.", "") + "_" + t2.replaceAll("\\.", "")
    }
  }

  /**
   * 计算区块中心点的UDF
   * 输入GeoHash位数/纬度/经度
   * 输出GeoHash方块的中心经纬度
   * @return
   */
  def geoHashCenterByLv: UDF3[Int, String, String, Row] = new UDF3[Int, String, String, Row] {
    override def call(lv: Int, lat: String, lon: String): Row = {
      val outT2 = instance.getGeoHashCenterByLv(lv)(lat.toDouble, lon.toDouble)
      Row.apply(
        outT2._1.toString.toDouble.formatted("%.6f").toString,
        outT2._2.toString.toDouble.formatted("%.6f").toString
      )
    }

    // usage:先定义schema文件
    //    val centerSchema: StructType = StructType(
    //      StructField("lat", StringType, nullable = false) ::
    //        StructField("lon", StringType, nullable = false) :: Nil)
    // 注册UDF
    //    hiveSqlContext.udf.register("center", GeoHashUDF.geoHashCenterByLv, centerSchema)
  }




  def main(args: Array[String]): Unit = {


    print(GeoHash.fromGeohashString("wz9du7").getBoundingBoxCenterPoint.getLatitude)
    print(GeoHash.fromGeohashString("wz9du7").getBoundingBoxCenterPoint.getLongitude)
//    val dis = getDistance(36.177107,117.099742,36.178250,117.101306)
//    val dis1 = getDistance(23.144576,113.29273,23.144092,113.292793)
//    println(dis)
//    println(dis1)

//    println(GeoHashUDF.getGeoHashCenterByLv(6)(28.118089,113.160159))

//    // 初始化 sparkContext
//    val conf = new SparkConf().setAppName("Spark Application -->  Poi Type Tag").setMaster("local")
//    val sc: SparkContext = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    import sqlContext.implicits._
//    val arr = Array((30.001, 110.000), (40.00, 120.00))
//
//    val rdd: RDD[(String, String)] = sc.parallelize(arr).map(t2 => (t2._1.toString, t2._2.toString))
//    rdd.toDF("lat", "lon").registerTempTable("test")
//
//
//    val schema: StructType = StructType(
//      StructField("around_0", StringType, nullable = false) ::
//        StructField("around_1", StringType, nullable = false) ::
//        StructField("around_2", StringType, nullable = false) ::
//        StructField("around_3", StringType, nullable = false) ::
//        StructField("around_4", StringType, nullable = false) ::
//        StructField("around_5", StringType, nullable = false) ::
//        StructField("around_6", StringType, nullable = false) ::
//        StructField("around_7", StringType, nullable = false) :: Nil)
//
//    sqlContext.udf.register("around", geoHashAroundByLvUDF, schema)
//    sqlContext.sql("SELECT g6(6,lat,lon) g6 FROM test").registerTempTable("t1")
//
//    sqlContext.sql("SELECT g6.around_0 FROM t1").show()

  }
}

class GeoHashUDF extends Serializable{

  /**
   * 计算两个经纬度之间的距离
   *
   * @return 距离
   */
  def getDistance: (Double, Double, Double, Double) => Double =
    (loc_lat: Double, loc_lng: Double, poi_lat: Double, poi_lng: Double) => {
      val EARTH_RADIUS: Double = 6367000.0
      val radLat1 = rad(loc_lat)
      val radLat2 = rad(poi_lat)
      val a = radLat1 - radLat2
      val b = rad(loc_lng) - rad(poi_lng)
      var s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
        + Math.cos(radLat1) * Math.cos(radLat2)
        * Math.pow(Math.sin(b / 2), 2)))
      s = s * EARTH_RADIUS
      s = Math.round(s * 1000000d) / 1000000d
      s
    }

  private def rad(d: Double): Double = d * Math.PI / 180.0


  /**
   * 求经纬度区块的中心点
   *
   * @param geoHashLv 位数
   * @return
   */
  def getGeoHashCenterByLv(geoHashLv: Int): (Double, Double) => (Double, Double) = (lat: Double, lon: Double) => {
    val s = GeoHash.geoHashStringWithCharacterPrecision(lat, lon, geoHashLv)
    val t2 = GeoHash.fromGeohashString(s).getBoundingBox.getCenterPoint
    (t2.getLatitude, t2.getLongitude)
  }

  /**
   * 计算经纬度区块的字符串标号
   *
   * @param geoHashLv 位数
   * @return
   */
  def getGeoHashBase32ByLv(geoHashLv: Int): (Double, Double) => String = (lat: Double, lon: Double) => {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lon, geoHashLv)
  }

  def getPointByGeoHashStr(geoHashStr: String): (Double, Double) = {

    val lat = GeoHash.fromGeohashString(geoHashStr).getBoundingBoxCenterPoint.getLatitude
    val lon = GeoHash.fromGeohashString(geoHashStr).getBoundingBoxCenterPoint.getLongitude
    (lat, lon)
  }

}
