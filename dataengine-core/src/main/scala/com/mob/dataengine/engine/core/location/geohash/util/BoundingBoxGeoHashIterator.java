package com.mob.dataengine.engine.core.location.geohash.util;




import com.mob.dataengine.engine.core.location.geohash.GeoHash;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Iterate over all of the values within a bounding box at a particular
 * resolution
 */
public class BoundingBoxGeoHashIterator implements Iterator<GeoHash> {
	private TwoGeoHashBoundingBox boundingBox;
	private GeoHash current;

	public BoundingBoxGeoHashIterator(TwoGeoHashBoundingBox bbox) {
		this.boundingBox = bbox;
		this.current = bbox.getBottomLeft();
	}

	public TwoGeoHashBoundingBox getBoundingBox() {
		return boundingBox;
	}

	@Override
	public boolean hasNext() {
		return current.compareTo(boundingBox.getTopRight()) <= 0;
	}

	@Override
	public GeoHash next() {
		GeoHash rv = current;
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		current = rv.next();
		while (hasNext() && !boundingBox.getBoundingBox().contains(current.getPoint())) {
			current = current.next();
		}
		return rv;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
