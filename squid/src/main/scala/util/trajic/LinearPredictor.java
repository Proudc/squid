package src.main.scala.util.trajic;

public class LinearPredictor extends Predictor {
    
    public float predictTime(float[][] tuples, int index) {
        float cur = tuples[index - 1][0];
        float prev = index > 1 ? tuples[index - 2][0] : cur;
        return (cur * 2 - prev);
    }

    public void predictCoords(float[][] tuples, int index, float[] result) {
        GPSPoint cur = new GPSPoint(tuples[index - 1]);
        GPSPoint prev = index > 1 ? new GPSPoint(tuples[index - 2]) : cur;

        float prevInterval = cur.time - prev.time;
        float interval = tuples[index][0] - cur.time;
        float ratio = prevInterval > 0 ? interval / prevInterval : 1;

        float time = predictTime(tuples, index);
        float lon = cur.lon + (cur.lon - prev.lon) * ratio;
        float lat = cur.lat + (cur.lat - prev.lat) * ratio;

        result[0] = time;
        result[1] = lon;
        result[2] = lat;
    }

    public float predictTime(int[][] tuples, int index) {
        float cur = Float.intBitsToFloat(tuples[index - 1][0]);
        float prev = index > 1 ? Float.intBitsToFloat(tuples[index - 2][0]) : cur;
        return (cur * 2 - prev);
    }

    public void predictCoords(int[][] tuples, int index, float[] result) {
        GPSPoint cur = new GPSPoint(Float.intBitsToFloat(tuples[index - 1][0]),
                                    Float.intBitsToFloat(tuples[index - 1][1]),
                                    Float.intBitsToFloat(tuples[index - 1][2]));
        GPSPoint prev = index > 1 ? new GPSPoint(Float.intBitsToFloat(tuples[index - 2][0]),
                                    Float.intBitsToFloat(tuples[index - 2][1]),
                                    Float.intBitsToFloat(tuples[index - 2][2])) : cur;

        float prevInterval = cur.time - prev.time;
        float interval = Float.intBitsToFloat(tuples[index][0]) - cur.time;
        double ratio = prevInterval > 0 ? interval / prevInterval : 1;

        float time = predictTime(tuples, index);
        float lon = (float) (cur.lon + (cur.lon - prev.lon) * ratio);
        float lat = (float) (cur.lat + (cur.lat - prev.lat) * ratio);

        result[0] = time;
        result[1] = lon;
        result[2] = lat;
        if (ratio - 1.0 > 0.0000001) {
            System.out.println(prevInterval + "\t" + interval + "\t" + ratio + "\t" + time);
        }
    }

    public void predictCoordsNoTime(float[][] tuples, int index, float[] result) {
        GPSPoint cur = new GPSPoint(tuples[index - 1]);
        GPSPoint prev = index > 1 ? new GPSPoint(tuples[index - 2]) : cur;

        double ratio = 1.0;

        float time = predictTime(tuples, index);
        float lon = (float) (cur.lon + (cur.lon - prev.lon) * ratio);
        float lat = (float) (cur.lat + (cur.lat - prev.lat) * ratio);

        result[0] = time;
        result[1] = lon;
        result[2] = lat;
    }

    public void predictCoordsNoTime(int[][] tuples, int index, float[] result) {
        GPSPoint cur = new GPSPoint(Float.intBitsToFloat(tuples[index - 1][0]),
                Float.intBitsToFloat(tuples[index - 1][1]), Float.intBitsToFloat(tuples[index - 1][2]));
        GPSPoint prev = index > 1
                ? new GPSPoint(Float.intBitsToFloat(tuples[index - 2][0]),
                        Float.intBitsToFloat(tuples[index - 2][1]), Float.intBitsToFloat(tuples[index - 2][2]))
                : cur;

        double ratio = 1.0;

        float time = predictTime(tuples, index);
        float lon = (float) (cur.lon + (cur.lon - prev.lon) * ratio);
        float lat = (float) (cur.lat + (cur.lat - prev.lat) * ratio);

        result[0] = time;
        result[1] = lon;
        result[2] = lat;
    }

}
