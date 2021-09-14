package src.main.scala.util.trajic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import java.util.ArrayList;

public class TXTReader extends GPSReader {

    public TXTReader(String fileName) {
        super(fileName);
    }

    public ArrayList<GPSPoint> readPoints() {
        ArrayList<GPSPoint> points = new ArrayList<GPSPoint>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(this.fileName)));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] lines = line.split(",");
                // double time = (double) PublicFunc.convertTime(lines[1], lines[2],
                // "2015-04-01", "00:00:00");
                // GPSPoint point = new GPSPoint(time, Double.valueOf(lines[3]),
                // Double.valueOf(lines[4]));
                GPSPoint point = new GPSPoint(Float.valueOf(lines[0]), Float.valueOf(lines[1]),
                        Float.valueOf(lines[2]));
                points.add(point);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return points;
    }

}
