package com.mob.dataengine.utils.geohash;

import java.io.Serializable;

public class BoundingBox implements Serializable {
    private static final long serialVersionUID = -7145192134410261076L;
    private double minLat;
    private double maxLat;
    private double minLon;
    private double maxLon;

    /**
     * create a bounding box defined by two coordinates
     */
    public BoundingBox(WGS84Point p1, WGS84Point p2) {
        this(p1.getLatitude(), p2.getLatitude(), p1.getLongitude(), p2.getLongitude());
    }

    public BoundingBox(double y1, double y2, double x1, double x2) {
        minLon = Math.min(x1, x2);
        maxLon = Math.max(x1, x2);
        minLat = Math.min(y1, y2);
        maxLat = Math.max(y1, y2);
    }


    private static int hashCode(double x) {
        long f = Double.doubleToLongBits(x);
        return (int) (f ^ (f >>> 32));
    }
}
