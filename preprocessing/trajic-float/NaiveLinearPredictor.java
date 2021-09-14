public class NaiveLinearPredictor{

    public float predictTime(float[][] tuples, int index) {
        double cur = tuples[index - 1][0];
        double prev = index > 1 ? tuples[index - 2][0] : cur;
        return (float) (cur * 2 - prev);
    }

    public void predictCoords(float[][] tuples, int index, float[] result) {
        GPSPoint cur = new GPSPoint(tuples[index - 1]);
        GPSPoint prev = index > 1 ? new GPSPoint(tuples[index - 2]) : cur;

        float time = predictTime(tuples, index);
        float lon = (float) (cur.lon * 2 - prev.lon);
        float lat = (float) (cur.lat * 2 - prev.lat);

        result[0] = time;
        result[1] = lon;
        result[2] = lat;
    }

}
