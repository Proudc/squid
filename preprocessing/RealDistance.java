// package util.realDistance;

import java.lang.Math;

/**
 * Calculate the real distance between two points on the earth
 */
public class RealDistance{

    /**
     * Earth radius
     */
    public static final double EARTH_RADIUS = 6378.137;

    /**
     * Auxiliary function
     */
    public static double haverSin(final double theta){
        double v = Math.sin(theta / 2);
        return v * v;
    }

    /**
     * Degree to radian function
     */
    public static double convertDegreeToRadian(final double degree){
        return degree * Math.PI / 180;
    }

    /**
     * Radian to degree function
     */
    public static double convertRadianToDegree(final double radian){
        return radian * 180.0 / Math.PI;
    }

    /**
     * The most accurate distance function
     * 
     * @param lon1
     * @param lat1
     * @param lon2
     * @param lat2
     * @return real Distance
     */
    public static double realDistance(final double lon1, final double lat1, final double lon2, final double lat2){
        double lonRadian1 = convertDegreeToRadian(lon1);
        double latRadian1 = convertDegreeToRadian(lat1);
        double lonRadian2 = convertDegreeToRadian(lon2);
        double latRadian2 = convertDegreeToRadian(lat2);
        double lonDiff  = Math.abs(lonRadian1 - lonRadian2);
        double latDiff  = Math.abs(latRadian1 - latRadian2);
        double height   = haverSin(latDiff) + Math.cos(latRadian1) * Math.cos(latRadian2) * haverSin(lonDiff);
        double distance = 2 * EARTH_RADIUS * Math.asin(Math.sqrt(height));
        return distance * 1000;
    }

    /**
     * The function of calculating distance with large error used in continuous
     * query
     * 
     * @param lon1
     * @param lat1
     * @param lon2
     * @param lat2
     * @return real Distance as a comparison
     */
    public static double compareRealDistance(final double lon1, final double lat1, final double lon2, final double lat2){
        final double DISTANCE_ONE_DEGREE = 111111;
        double lonDiff = lon1 - lon2;
        double latDiff = lat1 - lat2;
        double lonDistance = lonDiff * DISTANCE_ONE_DEGREE * Math.cos(lat1);
        double latDistance = latDiff * DISTANCE_ONE_DEGREE;
        double distance = Math.sqrt(lonDistance * lonDistance + latDistance * latDistance);
        return distance;
    }

    /**
     * Calculate the Euclidean distance between two points
     */
    public static double eucDistance(final double lon1, final double lat1, final double lon2, final double lat2){
        double lonDiff = lon1 - lon2;
        double latDiff = lat1 - lat2;
        return Math.sqrt(lonDiff * lonDiff + latDiff * latDiff);
    }

}