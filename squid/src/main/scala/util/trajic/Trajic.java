package src.main.scala.util.trajic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.File;
import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import src.main.scala.util.trajic.GPSPoint;

public class Trajic {

    public static void main(String[] args) {
        // String mode   = args[0];
        // String infile = args[1];
        // float mte = Float.valueOf(args[2]);
        // float mse = Float.valueOf(args[3]);
        // if (mode.equals("c")) {
        //     compress(infile, mte, mse);
        // } else {
        //     decompress(infile);
        //     // decompressByteArray(readByte(infile));
        // }
        String rootReadPath1 = "/mnt/data8/singapore_drive_data/realTraj/2015-04-01-zorder/";
        String rootWritePath1 = "/mnt/data8/singapore_drive_data/realTraj/2015-04-01-trajic-format1-whole/";
        String rootWritePath2 = "/mnt/data8/singapore_drive_data/realTraj/2015-04-01-trajic-format1-onebyone/";
        int timeParsNum = 48;
        int spaceParsNum = 100;
        compressWholeNoTimeEntry(rootReadPath1, rootWritePath1, timeParsNum, spaceParsNum, 1, 360);
        compressOneByOneNoTimeEntry(rootReadPath1, rootWritePath2, timeParsNum, spaceParsNum, 1, 260, 360);
    }
    
    public static void compress(final String fileName, float mte, float mse) {
        PredictiveCompressor c = new PredictiveCompressor(mte, mse);
        
        System.out.println("Reading file...");
        ArrayList<GPSPoint> points = readPoints(fileName);
        
        System.out.println("Compressing...");
        String writeFileName = PublicFunc.fileBaseName(fileName) + ".tjc";
        
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(writeFileName));
            Obstream obs = new Obstream(out);
            // c.compress(obs, points);
            c.compressNoTime(obs, points);
            System.out.println(obs.totalBits);
            System.out.println(obs.totalBytes);
            obs.close();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Done.");
    }

    public static void decompress(final String fileName) {
        PredictiveCompressor c = new PredictiveCompressor();

        System.out.println("Decompressing...");
        String writeFileName = PublicFunc.fileBaseName(fileName) + ".txt";
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(fileName));
            Ibstream ibs = new Ibstream(in);
            ArrayList<GPSPoint> points = c.decompress(ibs);
            in.close();
            System.out.println("Writing file...");
            writePoints(writeFileName, points);
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        System.out.println("Done.");
    }

    public static ArrayList<GPSPoint> readPoints(String fileName) {
        GPSReader reader;
        
        if (PublicFunc.fileExt(fileName).equals("csv")) {
            reader = new CSVReader(fileName);
        } else {
            reader = new TXTReader(fileName);
        }
        
        return reader.readPoints();
    }

    public static void writePoints(String fileName, ArrayList<GPSPoint> points) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName)));
            for (int i = 0; i < points.size(); i++) {
                GPSPoint point = points.get(i);
                writer.write(point.time + "\t" + point.lon + "\t" + point.lat + "\n");
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void decompressByteArray(byte[] inputData) {
        PredictiveCompressor c = new PredictiveCompressor();
        
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(inputData));
            Ibstream ibs = new Ibstream(in);
            ArrayList<GPSPoint> points = c.decompress(ibs);
            in.close();
            for (int i = 0; i < points.size(); i++) {
                GPSPoint point = points.get(i);
                // System.out.println(point.time + "\t" + point.lon + "\t" + point.lat);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    // Read bytes from the given file
    public static byte[] readByte(final String path) {
        ArrayList<Byte> data = new ArrayList<>();
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
            byte[] tem = new byte[1];
            while (in.read(tem) != -1) {
                data.add(tem[0]);
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        byte[] returnData = new byte[data.size()];
        for (int i = 0; i < data.size(); i++) {
            returnData[i] = data.get(i);
        }
        return returnData;
    }

    public static byte[] decompressByteArrayNoTime(byte[] inputData) {
        PredictiveCompressor c = new PredictiveCompressor();
        ArrayList<Byte> data = new ArrayList<>();
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(inputData));
            Ibstream ibs = new Ibstream(in);
            ArrayList<GPSPoint> points = c.decompressNoTime(ibs);
            in.close();
            for (int i = 0; i < points.size(); i++) {
                GPSPoint point = points.get(i);
                byte[] lonBytesArray = PublicFunc.floatToBytes(point.lon);
                byte[] latBytesArray = PublicFunc.floatToBytes(point.lat);
                for (int j = 0; j < lonBytesArray.length; j++) {
                    data.add(lonBytesArray[j]);
                }
                for (int j = 0; j < latBytesArray.length; j++) {
                    data.add(latBytesArray[j]);
                }
                // System.out.println(point.time + "\t" + point.lon + "\t" + point.lat);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        byte[] returnData = new byte[data.size()];
        for (int i = 0; i < data.size(); i++) {
            returnData[i] = data.get(i);
        }
        // byte[] returnData = new byte[2880];
        return returnData;
    }

    public static ArrayList<GPSPoint> decompressByteArrayNoTimeGetPoints(byte[] inputData) {
        PredictiveCompressor c = new PredictiveCompressor();
        ArrayList<GPSPoint> points = null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(inputData));
            Ibstream ibs = new Ibstream(in);
            points = c.decompressNoTime(ibs);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return points;
    }

    public static void compressPointsNoTime(ArrayList<GPSPoint> points, String writeFileName, float mte, float mse) {
        PredictiveCompressor c = new PredictiveCompressor(mte, mse);

        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(writeFileName));
            Obstream obs = new Obstream(out);
            // c.compress(obs, points);
            c.compressNoTime(obs, points);
            // System.out.println(obs.totalBits);
            // System.out.println(obs.totalBytes);
            obs.close();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    public static void compressWholeNoTimeEntry(String rootReadPath, String rootWritePath, int timeParsNum,
            int spaceParsNum, int storageFormat, int pointsEachParEachTraj) {
        for (int i = 0; i < timeParsNum; i++) {
            for (int j = 0; j < spaceParsNum; j++) {
                String readPath = rootReadPath + "par_ori" + String.valueOf(i) + "zorder" + String.valueOf(j)
                        + ".tstjs";
                String writePath = rootWritePath + "par_ori" + String.valueOf(i) + "zorder" + String.valueOf(j)
                        + ".tstjs";
                compressWholeNoTime(readPath, writePath, storageFormat, pointsEachParEachTraj);
            }
        }
    }

    private static void compressWholeNoTime(String readPath, String writePath, int storageFormat, int pointsEachParEachTraj) {
        if (storageFormat == 1) {
            ArrayList<GPSPoint> points = new ArrayList<>();
            try {
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(readPath)));
                byte[] tem = new byte[8];
                while (in.read(tem) != -1) {
                    float lon = ByteBuffer.wrap(Arrays.copyOfRange(tem, 0, 4)).getFloat();
                    float lat = ByteBuffer.wrap(Arrays.copyOfRange(tem, 4, 8)).getFloat();
                    GPSPoint point = new GPSPoint(0.0f, lon, lat);
                    points.add(point);
                }
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            compressPointsNoTime(points, writePath, 0.0f, 0.0f);
        } else if (storageFormat == 2) {
            ArrayList<GPSPoint> points = new ArrayList<>();
            try {
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(readPath)));
                int eachTrajByteLength = pointsEachParEachTraj * 8;
                byte[] tem = new byte[eachTrajByteLength];
                while (in.read(tem) != -1) {
                    for (int i = 0; i < pointsEachParEachTraj; i++) {
                        float lon = ByteBuffer.wrap(Arrays.copyOfRange(tem, i * 4, (i + 1) * 4)).getFloat();
                        float lat = ByteBuffer.wrap(Arrays.copyOfRange(tem, i * 4 + (eachTrajByteLength / 2), (i + 1) * 4 + (eachTrajByteLength / 2))).getFloat();
                        GPSPoint point = new GPSPoint(0.0f, lon, lat);
                        points.add(point);
                    }
                }
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            compressPointsNoTime(points, writePath, 0.0f, 0.0f);
        } else {
            System.out.println("Storage format is neither 1 nor 2");
        }
    }

    public static void compressOneByOneNoTimeEntry(String rootReadPath, String rootWritePath, int timeParsNum,
            int spaceParsNum, int storageFormat, int trajNumEachPar, int pointsEachParEachTraj) {
        for (int i = 0; i < timeParsNum; i++) {
            for (int j = 0; j < spaceParsNum; j++) {
                String readPath = rootReadPath + "par_ori" + String.valueOf(i) + "zorder" + String.valueOf(j)
                        + ".tstjs";
                String writePath = rootWritePath + "par_ori" + String.valueOf(i) + "zorder" + String.valueOf(j)
                        + ".tstjs";
                compressOneByOneNoTime(readPath, writePath, storageFormat, trajNumEachPar, pointsEachParEachTraj);
            }
        }
    }

    private static void compressOneByOneNoTime(String readPath, String writePath, int storageFormat,
            int trajNumEachPar, int pointsEachParEachTraj) {
        if (storageFormat == 1) {
            byte[] writeData = new byte[trajNumEachPar * pointsEachParEachTraj * 8];
            byte[] controlInfor = new byte[trajNumEachPar * 4];
            int dataInforPos = 0;
            int controlInforPos = 0;
            String temPath = "/mnt/data8/singapore_drive_data/realTraj/tem.tstjs";
            try {
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(readPath)));
                int eachTrajByteLength = pointsEachParEachTraj * 8;
                byte[] tem = new byte[eachTrajByteLength];
                while (in.read(tem) != -1) {
                    ArrayList<GPSPoint> points = new ArrayList<>();
                    for (int i = 0; i < pointsEachParEachTraj; i++) {
                        float lon = ByteBuffer.wrap(Arrays.copyOfRange(tem, i * 8, i * 8 + 4)).getFloat();
                        float lat = ByteBuffer.wrap(Arrays.copyOfRange(tem, i * 8 + 4, (i + 1) * 8)).getFloat();
                        GPSPoint point = new GPSPoint(0.0f, lon, lat);
                        points.add(point);
                    }
                    compressPointsNoTime(points, temPath, 0.0f, 0.0f);
                    byte[] afterZipData = readByte(temPath);
                    byte[] posByteArray = intToByteArrayHigh(dataInforPos);
                    for (int j = 0; j < posByteArray.length; j++) {
                        controlInfor[controlInforPos] = posByteArray[j];
                        controlInforPos += 1;
                    }
                    for (int j = 0; j < afterZipData.length; j++) {
                        writeData[dataInforPos] = afterZipData[j];
                        dataInforPos += 1;
                    }
                }
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            byte[] finalData = new byte[controlInforPos + dataInforPos];
            int finalPos = 0;
            for (int i = 0; i < controlInforPos; i++) {
                finalData[finalPos] = controlInfor[i];
                finalPos += 1;
            }
            for (int i = 0; i < dataInforPos; i++) {
                finalData[finalPos] = writeData[i];
                finalPos += 1;
            }
            writeByteArray(writePath, finalData, finalPos);
        } else if (storageFormat == 2) {
            byte[] writeData = new byte[trajNumEachPar * pointsEachParEachTraj * 8];
            byte[] controlInfor = new byte[trajNumEachPar * 4];
            int dataInforPos = 0;
            int controlInforPos = 0;
            String temPath = "";
            try {
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(readPath)));
                int eachTrajByteLength = pointsEachParEachTraj * 8;
                byte[] tem = new byte[eachTrajByteLength];
                while (in.read(tem) != -1) {
                    ArrayList<GPSPoint> points = new ArrayList<>();
                    for (int i = 0; i < pointsEachParEachTraj; i++) {
                        float lon = ByteBuffer.wrap(Arrays.copyOfRange(tem, i * 4, (i + 1) * 4)).getFloat();
                        float lat = ByteBuffer.wrap(Arrays.copyOfRange(tem, i * 4 + (eachTrajByteLength / 2),
                                (i + 1) * 4 + (eachTrajByteLength / 2))).getFloat();
                        GPSPoint point = new GPSPoint(0.0f, lon, lat);
                        points.add(point);
                    }
                    compressPointsNoTime(points, temPath, 0.0f, 0.0f);
                    byte[] afterZipData = readByte(temPath);
                    byte[] posByteArray = intToByteArrayHigh(dataInforPos);
                    for (int j = 0; j < posByteArray.length; j++) {
                        controlInfor[controlInforPos] = posByteArray[j];
                        controlInforPos += 1;
                    }
                    for (int j = 0; j < afterZipData.length; j++) {
                        writeData[dataInforPos] = afterZipData[j];
                        dataInforPos += 1;
                    }
                }
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            byte[] finalData = new byte[controlInforPos + dataInforPos];
            int finalPos = 0;
            for (int i = 0; i < controlInforPos; i++) {
                finalData[finalPos] = controlInfor[i];
                finalPos += 1;
            }
            for (int i = 0; i < dataInforPos; i++) {
                finalData[finalPos] = writeData[i];
                finalPos += 1;
            }
            writeByteArray(writePath, finalData, finalPos);
        } else {
            System.out.println("Storage format is neither 1 nor 2");
        }
    }

    public static byte[] intToByteArrayHigh(int num) {
        byte[] b = new byte[4];
        b[3] = (byte) (num & 0xff);
        b[2] = (byte) (num >> 8 & 0xff);
        b[1] = (byte) (num >> 16 & 0xff);
        b[0] = (byte) (num >> 24 & 0xff);
        return b;
    }

    public static void writeByteArray(String writePath, byte[] data, int pos) {
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(writePath, false));
            byte[] newData = new byte[pos];
            for (int i = 0; i < pos; i++) {
                newData[i] = data[i];
            }
            out.write(newData);
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
