package com.mob.dataengine.engine.core.portrait;

/**
 * @author juntao zhang
 */

public abstract class AdjustProcessor {

  public abstract double[][] process(
      String[] keys, double matchIOSCount, double[] androidCounts,
      double[] androidRatios, double[] iosRatios
  );
}