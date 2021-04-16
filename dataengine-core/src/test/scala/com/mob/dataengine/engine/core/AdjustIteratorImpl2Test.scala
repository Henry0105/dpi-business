package com.mob.dataengine.engine.core

/**
 * @author juntao zhang
 */
object AdjustIteratorImpl2Test {

  def process(keys: Array[String], matchIOSCount: Double, androidCounts: Array[Double],
    androidRatios: Array[Double], iosRatios: Array[Double]): Array[Array[Double]] = {
    val androidTolCnt = androidCounts.sum
    val tmp2 = keys.indices.map(i => (androidCounts(i) * iosRatios(i)) / (androidTolCnt * androidRatios(i)))
    val tmp1 = tmp2.sum
    val iosCounts = tmp2.map(t => ((t / tmp1) * matchIOSCount))
    val iosTolCount = iosCounts.sum
    val tolCnt = iosTolCount + androidTolCnt
    val finalPercents = keys.indices.map(i => (iosCounts(i) + androidCounts(i)) / tolCnt)
    Array(iosCounts.toArray, finalPercents.toArray)
  }

  def main(args: Array[String]): Unit = {
    println(
      """
        |public double[][] process(String[] keys, double matchIOSCount, double[] androidCounts, double[] androidRatios,
        |    double[] iosRatios) {
        |  double androidTolCnt = 0d;
        |  for (double d : androidCounts) {
        |    androidTolCnt += d;
        |  }
        |  double tmpTotal = 0d;
        |  double[] tmp = new double[keys.length];
        |  for (int i = 0; i < keys.length; i++) {
        |    tmp[i] = (androidCounts[i] * iosRatios[i]) / (androidTolCnt * androidRatios[i]);
        |    tmpTotal += tmp[i];
        |  }
        |  double[] iosCounts = new double[keys.length];
        |  double iosTolCount = 0d;
        |  for (int i = 0; i < keys.length; i++) {
        |    iosCounts[i] = Math.round((tmp[i] / tmpTotal) * matchIOSCount);
        |    iosTolCount += iosCounts[i];
        |  }
        |  double tolCnt = iosTolCount + androidTolCnt;
        |  double[] finalPercents = new double[keys.length];
        |  for (int i = 0; i < keys.length; i++) {
        |    finalPercents[i] = (iosCounts[i] + androidCounts[i]) / tolCnt;
        |  }
        |  return new double[][]{iosCounts, finalPercents};
        |}
      """.stripMargin)
  }
}
