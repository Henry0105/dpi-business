package com.mob.dataengine.engine;

import com.mob.dataengine.engine.core.portrait.AdjustProcessor;

/**
 * @author juntao zhang
 */
public class AdjustIteratorImplTest extends AdjustProcessor {

  //return iosCounts 最后占比
  public double[][] process(String[] keys, double matchIOSCount, double[] androidCounts, double[] androidRatios,
      double[] iosRatios) {
    Double androidTolCnt = 0d;
    for (Double d : androidCounts) {
      androidTolCnt += d;
    }
    double tmp1 = 0d;
    double[] tmp2 = new double[keys.length];
    for (int i = 0; i < keys.length; i++) {
      tmp2[i] = (androidCounts[i] * iosRatios[i]) / (androidTolCnt * androidRatios[i]);
      tmp1 += tmp2[i];
    }
    double[] iosCounts = new double[keys.length];
    Double iosTolCount = 0d;
    for (int i = 0; i < keys.length; i++) {
      iosCounts[i] = Math.round((tmp2[i] / tmp1) * matchIOSCount);
      iosTolCount += iosCounts[i];
    }
    Double tolCnt = iosTolCount + androidTolCnt;
    double[] finalPercents = new double[keys.length];
    for (int i = 0; i < keys.length; i++) {
      finalPercents[i] = ((iosCounts[i] + androidCounts[i]) / tolCnt);
    }
    return new double[][]{iosCounts, finalPercents};
  }
}