package src.main.scala.util.trajic;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.util.ArrayList;

public class PredictiveCompressor {
    
    public float maxTemporalError;
    
    public float maxSpatialError;

    public Predictor predictor;

    public PredictiveCompressor(Predictor predictor, float maxTemporalError, float maxSpatialError) {
        this.predictor        = predictor;
        this.maxTemporalError = maxTemporalError;
        this.maxSpatialError  = maxSpatialError;
    }

    public PredictiveCompressor(float maxTemporalError, float maxSpatialError) { 
        this(new LinearPredictor(), maxTemporalError, maxSpatialError);
    }

    public PredictiveCompressor() {
        this.predictor = new LinearPredictor();
    }

    public int calculateDiscardedBits(float maxValue, float errorBound) {
        int    bits = Float.floatToIntBits(maxValue);
        long   exponent = (bits & 0x7ff00000) >> 23;
        double tem = errorBound * Math.pow(2, 150 - exponent) + 1;
        return Math.min((int)(Math.log(tem) / Math.log(2.0)), 52);
    }

    public void compress(Obstream obs, ArrayList<GPSPoint> points) {
        float maxTime  = 0.0f;
        float maxCoord = 0.0f;
        
        for (GPSPoint point : points) {
            maxTime  = Math.max(maxTime, point.time);
            maxCoord = Math.max(maxCoord, point.lon);
            maxCoord = Math.max(maxCoord, point.lat);
        }

        int[] discard = new int[3];
        discard[0] = calculateDiscardedBits(maxTime, this.maxTemporalError);
        discard[1] = calculateDiscardedBits(maxCoord, this.maxSpatialError);
        discard[2] = discard[1];
        

        obs.writeInt(discard[0], 8);
        obs.writeInt(discard[1], 8);

        obs.writeInt(points.size(), 32);
        obs.writeFloat(points.get(0).time);
        obs.writeFloat(points.get(0).lon);
        obs.writeFloat(points.get(0).lat);
        System.out.println(discard[0] + "\t" + discard[1] + "\t" + discard[2] + "\t" + points.size());

        float[][] tuples = new float[points.size()][3];
        for (int i = 0; i < points.size(); i++) {
            tuples[i][0] = points.get(i).time;
            tuples[i][1] = points.get(i).lon;
            tuples[i][2] = points.get(i).lat;
        }

        int[][] residuals = new int[3][points.size() - 1];
        for (int i = 0; i < points.size() - 1; i++) {
            if (discard[0] > 0) {
                int predTime = Float.floatToIntBits(predictor.predictTime(tuples, i + 1));
                int residual = Float.floatToIntBits(tuples[i + 1][0]) ^ predTime;
                residual = (residual >>> discard[0]) << discard[0];
                tuples[i + 1][0] = Float.intBitsToFloat(predTime ^ residual);
            }
            
            float[] pred = new float[3];
            predictor.predictCoords(tuples, i + 1, pred);

            for (int j = 0 ; j < 3; j++) {
                int residual = Float.floatToIntBits(tuples[i + 1][j]) ^ Float.floatToIntBits(pred[j]);
                residual >>>= discard[j];
                residuals[j][i] = residual;
                residual <<= discard[j];
                tuples[i + 1][j] = Float.intBitsToFloat(Float.floatToIntBits(pred[j]) ^ residual);
            }
        }


        Encoder[] encoders = new Encoder[3];

        for (int j = 0; j < 3; j++) {
            encoders[j] = new DynamicEncoder(obs, residuals[j], points.size() - 1);
        }

        int timeBytes = 0;
        int lonBytes  = 0;
        int latBytes  = 0;
        for (int i = 0; i < points.size() - 1; i++) {
            for (int j = 0; j < 3; j++) {
                int beforeBytes = obs.numOfBytesWritten();
                encoders[j].encode(obs, residuals[j][i]);
                int afterBytes  = obs.numOfBytesWritten();
                if (j == 0) {
                    timeBytes += (afterBytes - beforeBytes);
                } else if (j == 1) {
                    lonBytes += (afterBytes - beforeBytes);
                } else {
                    latBytes += (afterBytes - beforeBytes);
                }
            }
        }
        System.out.println(timeBytes + "\t" + lonBytes + "\t" + latBytes);
    }

    public ArrayList<GPSPoint> decompress(Ibstream ibs) {
        int[] discard = new int[3];
        discard[0] = ibs.readByte();
        discard[1] = ibs.readByte();
        discard[2] = discard[1];
        
        int nPoints = (int) ibs.readInt(32);
        System.out.println(discard[0] + "\t" + discard[1] + "\t" + discard[2] + "\t" + nPoints);

        int[][] tuples = new int[nPoints][3];
        for (int i = 0; i < 3; i++) {
            tuples[0][i] = (int) ibs.readInt(32);
        }

        Encoder[] decoders = new Encoder[3];
        for (int i = 0; i < 3; i++) {
            decoders[i] = new DynamicEncoder(ibs);
        }
        for (int i = 1; i < nPoints; i++) {
            int[] residuals = new int[3];
            for (int j = 0; j < 3; j++) {
                int temNum = (int) decoders[j].decode(ibs);
                residuals[j] = temNum << discard[j];
            }
            int time = Float.floatToIntBits(this.predictor.predictTime(tuples, i)) ^ residuals[0];
            tuples[i][0] = time;

            float[] pred = new float[3];
            predictor.predictCoords(tuples, i, pred);

            for (int j = 0; j < 3; j++) {
                tuples[i][j] = Float.floatToIntBits(pred[j]) ^ residuals[j];
            }
        }


        ArrayList<GPSPoint> points = new ArrayList<GPSPoint>();
        for (int i = 0; i < nPoints; i++) {
            float time = Float.intBitsToFloat(tuples[i][0]);
            float lon = Float.intBitsToFloat(tuples[i][1]);
            float lat = Float.intBitsToFloat(tuples[i][2]);
            GPSPoint point = new GPSPoint(time, lon, lat);
            System.out.println(time + "\t" + lon + "\t" + lat);
            points.add(point);
        }

        return points;
    }

    public void compressNoTime(Obstream obs, ArrayList<GPSPoint> points) {
        float maxTime  = 0.0f;
        float maxCoord = 0.0f;

        for (GPSPoint point : points) {
            maxTime  = Math.max(maxTime, point.time);
            maxCoord = Math.max(maxCoord, point.lon);
            maxCoord = Math.max(maxCoord, point.lat);
        }

        int[] discard = new int[3];
        discard[0] = calculateDiscardedBits(maxTime, this.maxTemporalError);
        discard[1] = calculateDiscardedBits(maxCoord, this.maxSpatialError);
        discard[2] = discard[1];

        obs.writeInt(discard[0], 8);
        obs.writeInt(discard[1], 8);

        obs.writeInt(points.size(), 32);
        obs.writeFloat(points.get(0).time);
        obs.writeFloat(points.get(0).lon);
        obs.writeFloat(points.get(0).lat);
        System.out.println(discard[0] + "\t" + discard[1] + "\t" + discard[2] + "\t" + points.size());

        float[][] tuples = new float[points.size()][3];
        for (int i = 0; i < points.size(); i++) {
            tuples[i][0] = points.get(i).time;
            tuples[i][1] = points.get(i).lon;
            tuples[i][2] = points.get(i).lat;
        }

        int[][] residuals = new int[3][points.size() - 1];
        for (int i = 0; i < points.size() - 1; i++) {
            if (discard[0] > 0) {
                int predTime = Float.floatToIntBits(predictor.predictTime(tuples, i + 1));
                int residual = Float.floatToIntBits(tuples[i + 1][0]) ^ predTime;
                residual = (residual >>> discard[0]) << discard[0];
                tuples[i + 1][0] = Float.intBitsToFloat(predTime ^ residual);
            }

            float[] pred = new float[3];
            predictor.predictCoordsNoTime(tuples, i + 1, pred);

            for (int j = 0; j < 3; j++) {
                int residual = Float.floatToIntBits(tuples[i + 1][j]) ^ Float.floatToIntBits(pred[j]);
                residual >>>= discard[j];
                residuals[j][i] = residual;
                residual <<= discard[j];
                tuples[i + 1][j] = Float.intBitsToFloat(Float.floatToIntBits(pred[j]) ^ residual);
            }
        }

        Encoder[] encoders = new Encoder[3];

        for (int j = 1; j < 3; j++) {
            encoders[j] = new DynamicEncoder(obs, residuals[j], points.size() - 1);
        }

        for (int i = 0; i < points.size() - 1; i++) {
            for (int j = 1; j < 3; j++) {
                encoders[j].encode(obs, residuals[j][i]);
            }
        }
    }

    public ArrayList<GPSPoint> decompressNoTime(Ibstream ibs) {
        int[] discard = new int[3];
        discard[0] = ibs.readByte();
        discard[1] = ibs.readByte();
        discard[2] = discard[1];

        int nPoints = (int) ibs.readInt(32);
        System.out.println(nPoints);
        int[][] tuples = new int[nPoints][3];
        for (int i = 0; i < 3; i++) {
            tuples[0][i] = (int) ibs.readInt(32);
        }

        Encoder[] decoders = new Encoder[3];
        for (int i = 1; i < 3; i++) {
            decoders[i] = new DynamicEncoder(ibs);
        }
        for (int i = 1; i < nPoints; i++) {
            int[] residuals = new int[3];
            for (int j = 1; j < 3; j++) {
                int temNum = (int) decoders[j].decode(ibs);
                residuals[j] = temNum << discard[j];
            }
            int time = Float.floatToIntBits(this.predictor.predictTime(tuples, i)) ^ residuals[0];
            tuples[i][0] = time;

            float[] pred = new float[3];
            predictor.predictCoordsNoTime(tuples, i, pred);

            for (int j = 0; j < 3; j++) {
                tuples[i][j] = Float.floatToIntBits(pred[j]) ^ residuals[j];
            }
        }

        ArrayList<GPSPoint> points = new ArrayList<GPSPoint>();
        for (int i = 0; i < nPoints; i++) {
            float time = Float.intBitsToFloat(tuples[i][0]);
            float lon = Float.intBitsToFloat(tuples[i][1]);
            float lat = Float.intBitsToFloat(tuples[i][2]);
            GPSPoint point = new GPSPoint(time, lon, lat);
            points.add(point);
        }

        return points;
    }

}
