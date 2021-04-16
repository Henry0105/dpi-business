package com.mob.dataengine.commons.utils

import java.lang.Math.abs

/**
 * @author zhangnw
 */
object PoiUtil {
  def main(args: Array[String]): Unit = {
    val poly_line = "116.107612,39.897445;116.404844,40.052704;116.553172,39.91737;116.487057,39.837192;" +
      "116.380122,39.851374"
    // lons
    val polygonXA = for (item <- poly_line.split(";")) yield item.split(",")(0).toDouble
    // lats
    val polygonYA = for (item <- poly_line.split(";")) yield item.split(",")(1).toDouble

    val lls = Array(
      (116.356551, 39.993916),
      (116.350227, 39.93065),
      (116.392196, 39.925781),
      (116.165104, 40.031935))

    lls.foreach(t2 => {
      // 注意经度在前 维度在后
      val res0 = isIn(t2._1, t2._2, polygonXA, polygonYA)

      println(res0)
    })
  }

  def isPointOnLine(px0: Double, py0: Double, px1: Double, py1: Double, px2: Double, py2: Double): Boolean = {
    val ESP = 1e-9 // 无限小的正数
    if ((Math.abs(Multiply(px0, py0, px1, py1, px2, py2)) < ESP) && ((px0 - px1) * (px0 - px2) <= 0)
      && ((py0 - py1) * (py0 - py2) <= 0)) {
      true
    } else false
  }

  def Multiply(px0: Double, py0: Double, px1: Double, py1: Double, px2: Double, py2: Double): Double = {
    (px1 - px0) * (py2 - py0) - (px2 - px0) * (py1 - py0)
  }

  def isIn(lon: Double, lat: Double, polygonXA: Seq[Double], polygonYA: Seq[Double]): Boolean = {
    // 不构成多边形
    if (polygonXA.length < 3 || polygonYA.length < 3 || polygonXA.length != polygonYA.length) {
      false
    } else {
      var is_in = 0
      var intSum = 0

      for (index <- polygonXA.indices) {
        val i_1 = index
        val i_2 = if (index != polygonXA.length - 1) {
          index + 1
        } else 0
        val cx1 = polygonXA(i_1)
        val cy1 = polygonYA(i_1)
        val cx2 = polygonXA(i_2)
        val cy2 = polygonYA(i_2)

        if (isPointOnLine(lon, lat, cx1, cy1, cx2, cy2)) {
          is_in = 1
        } else {
          // 在顶点上时,只有边在射线上方时才加1;
          if ((lon > cx1 && lon < cx2) || (lon > cx2 && lon < cx1)) {
            // x在两点之间
            // 解求交点x;
            if (abs(cx1 - cx2) > 0) {
              val y = cy1 - (cy1 - cy2) * (cx1 - lon) / (cx1 - cx2); // 直线斜率 k = (y0-y1)/(x0-x1);
              if (y < lat) {
                // 存在交点在该点的左边(-无穷方向),若为(+无穷方向则为x>pt.x判定有交点.)
                intSum += 1
              }
            }
          }
        }
      }
      if (is_in == 1) {
        true
      } else {
        if (intSum % 2 == 1) true else false
      }
    }
  }
}
