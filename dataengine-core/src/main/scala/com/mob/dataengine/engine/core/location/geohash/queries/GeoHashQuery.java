package com.mob.dataengine.engine.core.location.geohash.queries;

import com.mob.dataengine.engine.core.location.geohash.GeoHash;
import com.mob.dataengine.engine.core.location.geohash.WGS84Point;

import java.util.List;


public interface GeoHashQuery {

    /**
     * check wether a geohash is within the hashes that make up this query.
     */
    public boolean contains(GeoHash hash);

    /**
     * returns whether a point lies within a query.
     */
    public boolean contains(WGS84Point point);

    /**
     * should return the hashes that re required to perform this search.
     */
    public List<GeoHash> getSearchHashes();

    public String getWktBox();

}