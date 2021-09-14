import java.util.ArrayList;

public abstract class GPSReader {
    
    public String fileName;

    public GPSReader(String fileName) {
        this.fileName = fileName;
    }

    public abstract ArrayList<GPSPoint> readPoints();

}
