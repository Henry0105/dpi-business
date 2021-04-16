package com.mob.dataengine.utils.geohash;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("javadoc")
public final class GeoHash implements Comparable<GeoHash>, Serializable {
    private static final int MAX_BIT_PRECISION = 64;
    private static final int MAX_CHARACTER_PRECISION = 12;

    private static final long serialVersionUID = -8553214249630252175L;
    public static final long FIRST_BIT_FLAGGED = 0x8000000000000000L;
    private static final char[] base32 = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    private final static Map<Character, Integer> decodeMap = new HashMap<>();

    static {
        int sz = base32.length;
        for (int i = 0; i < sz; i++) {
            decodeMap.put(base32[i], i);
        }
    }

    protected long bits = 0;
    private WGS84Point point;

    private BoundingBox boundingBox;

    protected byte significantBits = 0;

    /**
     * This method uses the given number of characters as the desired precision
     * value. The hash can only be 64bits long, thus a maximum precision of 12
     * characters can be achieved.
     */
    public static GeoHash withCharacterPrecision(double latitude, double longitude, int numberOfCharacters) {
        if (numberOfCharacters > MAX_CHARACTER_PRECISION) {
            throw new IllegalArgumentException("A geohash can only be " + MAX_CHARACTER_PRECISION + " character long.");
        }
        int desiredPrecision = (numberOfCharacters * 5 <= 60) ? numberOfCharacters * 5 : 60;
        return new GeoHash(latitude, longitude, desiredPrecision);
    }


    /**
     * This method uses the given number of characters as the desired precision
     * value. The hash can only be 64bits long, thus a maximum precision of 12
     * characters can be achieved.
     */
    public static String geoHashStringWithCharacterPrecision(double latitude, double longitude, int numberOfCharacters) {
        GeoHash hash = withCharacterPrecision(latitude, longitude, numberOfCharacters);
        return hash.toBase32();
    }

    private GeoHash(double latitude, double longitude, int desiredPrecision) {
        point = new WGS84Point(latitude, longitude);
        desiredPrecision = Math.min(desiredPrecision, MAX_BIT_PRECISION);

        boolean isEvenBit = true;
        double[] latitudeRange = {-90, 90};
        double[] longitudeRange = {-180, 180};

        while (significantBits < desiredPrecision) {
            if (isEvenBit) {
                divideRangeEncode(longitude, longitudeRange);
            } else {
                divideRangeEncode(latitude, latitudeRange);
            }
            isEvenBit = !isEvenBit;
        }

        setBoundingBox(this, latitudeRange, longitudeRange);
        bits <<= (MAX_BIT_PRECISION - desiredPrecision);
    }

    private static void setBoundingBox(GeoHash hash, double[] latitudeRange, double[] longitudeRange) {
        hash.boundingBox = new BoundingBox(new WGS84Point(latitudeRange[0], longitudeRange[0]), new WGS84Point(
                latitudeRange[1],
                longitudeRange[1]));
    }

    private void divideRangeEncode(double value, double[] range) {
        double mid = (range[0] + range[1]) / 2;
        if (value >= mid) {
            addOnBitToEnd();
            range[0] = mid;
        } else {
            addOffBitToEnd();
            range[1] = mid;
        }
    }


    /**
     * get the base32 string for this {@link GeoHash}.<br>
     * this method only makes sense, if this hash has a multiple of 5
     * significant bits.
     *
     * @throws IllegalStateException when the number of significant bits is not a multiple of 5.
     */
    public String toBase32() {
        if (significantBits % 5 != 0) {
            throw new IllegalStateException("Cannot convert a geohash to base32 if the precision is not a multiple of 5.");
        }
        StringBuilder buf = new StringBuilder();

        long firstFiveBitsMask = 0xf800000000000000l;
        long bitsCopy = bits;
        int partialChunks = (int) Math.ceil(((double) significantBits / 5));

        for (int i = 0; i < partialChunks; i++) {
            int pointer = (int) ((bitsCopy & firstFiveBitsMask) >>> 59);
            buf.append(base32[pointer]);
            bitsCopy <<= 5;
        }
        return buf.toString();
    }


    protected final void addOnBitToEnd() {
        significantBits++;
        bits <<= 1;
        bits = bits | 0x1;
    }

    protected final void addOffBitToEnd() {
        significantBits++;
        bits <<= 1;
    }

    @Override
    public int compareTo(GeoHash o) {
        int bitsCmp = Long.compare(bits ^ FIRST_BIT_FLAGGED, o.bits ^ FIRST_BIT_FLAGGED);
        if (bitsCmp != 0) {
            return bitsCmp;
        } else {
            return Integer.compare(significantBits, o.significantBits);
        }
    }
}
