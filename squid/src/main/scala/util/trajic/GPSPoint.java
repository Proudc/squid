package src.main.scala.util.trajic;

public class GPSPoint {

    public float time;
    public float lon;
    public float lat;

    public static final int EARTH_RADIUS = 6371;
    
    public GPSPoint(float time, float lon, float lat) {
        this.time = time;
        this.lon  = lon;
        this.lat  = lat;
    }

    public GPSPoint() {
        this((float) 0.0, (float) 0.0, (float) 0.0);
    }

    public GPSPoint(float[] triple) {
        this(triple[0], triple[1], triple[2]);
    }

    public double distance(GPSPoint other) {
        return Math.sqrt(Math.pow(other.lon - this.lon, 2) +
                         Math.pow(other.lat - this.lat, 2));
    }

    public double distanceKms(GPSPoint other) {
        float lon1 = (float) (this.lon  * Math.PI / 180);
        float lon2 = (float) (other.lon * Math.PI / 180);
        float lat1 = (float) (this.lat  * Math.PI / 180);
        float lat2 = (float) (other.lat * Math.PI / 180);

        float dlon = lon2 - lon1;
        float dlat = lat2 - lat1;
        double a = Math.sin(dlat / 2) * Math.sin(dlat / 2) +
                    Math.sin(dlon / 2) * Math.sin(dlon / 2) *
                    Math.cos(lat1) * Math.cos(lat2);
        
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS * c;
    }

}