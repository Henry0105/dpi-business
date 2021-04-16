package com.mob.dataengine.utils.geohash

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF3, UDF4}

import scala.collection.mutable.ArrayBuffer

/*
  * #Owner: zhangnw
  * #describe: GeoHash算法操作的UDF
  * #projectName:
  * #BusinessName:
  * #SourceTable:
  * #TargetTable:
  * #CreateDate: 2017/9/19 14:31
  */
object GeoHashUDF extends Serializable {

  val instance = new GeoHashUDF

  def apply: GeoHashUDF = instance

  private[this] val logger: Logger = Logger.getLogger(GeoHashUDF.getClass)


  def main(args: Array[String]): Unit = {
    // print(GeoHash.fromGeohashString("wz9du7").getBoundingBoxCenterPoint.getLatitude)
    // print(GeoHash.fromGeohashString("wz9du7").getBoundingBoxCenterPoint.getLongitude)
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

class GeoHashUDF extends Serializable {
  /*
    * 计算经纬度区块的字符串标号
    *
    * @param geoHashLv 位数
    * @return
    */
  def getGeoHashBase32ByLv(geoHashLv: Int): (Double, Double) => String = (lat: Double, lon: Double) => {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lon, geoHashLv)
  }

}

