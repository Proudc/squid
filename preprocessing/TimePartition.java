import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStream;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Calendar;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.nio.ByteBuffer;
import java.util.Arrays;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import java.util.*;

public class TimePartition {
    public static void main(String args[]) {
        String readPath     = args[0];
        String writePath    = args[1];
        int totalTrajNums   = Integer.valueOf(args[2]);
        int timePars        = Integer.valueOf(args[3]);
        int totalSnapOneDay = Integer.valueOf(args[4]);
        int batchSize       = Integer.valueOf(args[5]);
        cutTrajPartition(readPath, writePath, totalTrajNums, timePars, totalSnapOneDay, batchSize);
    }

    public static void cutTrajPartition(String rootReadPath, String rootWritePath, int totalTrajNums,
            int timePartitionsNum, int totalSnap, int batchSize) {
        System.out.println("Start to store the trajectory in partitions...");
        int pos = 0;
        while (pos < totalTrajNums) {
            System.out.println("Is processing " + (pos / batchSize) + "th batch...");
            int[][] idArrayTotal = new int[batchSize][totalSnap];
            short[][] secondArrayTotal = new short[batchSize][totalSnap];
            float[][] lonArrayTotal = new float[batchSize][totalSnap];
            float[][] latArrayTotal = new float[batchSize][totalSnap];
            int posTotal = 0;
            for (int i = pos; i < pos + batchSize; i++) {
                // System.out.println("Is processing " + i + "th trajectory...");
                String readPath = rootReadPath + String.valueOf(i) + ".tstjs";
                int[] idArray = new int[totalSnap];
                short[] secondArray = new short[totalSnap];
                float[] lonArray = new float[totalSnap];
                float[] latArray = new float[totalSnap];
                readTrajData(readPath, idArray, secondArray, lonArray, latArray);
                for (int j = 0; j < totalSnap; j++) {
                    idArrayTotal[posTotal][j] = idArray[j];
                    secondArrayTotal[posTotal][j] = secondArray[j];
                    lonArrayTotal[posTotal][j] = lonArray[j];
                    latArrayTotal[posTotal][j] = latArray[j];
                }
                posTotal += 1;
            }
            writeDataToFile(rootWritePath, idArrayTotal, secondArrayTotal, lonArrayTotal, latArrayTotal,
                    timePartitionsNum, batchSize);
            pos += batchSize;
        }
    }

    private static void readTrajData(String path, int[] idArray, short[] secondArray,
            float[] lonArray, float[] latArray) {
        byte[] readData = readByteArray(path);
        byte[] data = unGzip(readData);
        for (int i = 0; i < 17280; i++) {
            idArray[i] = ByteBuffer.wrap(Arrays.copyOfRange(data, i * 14, i * 14 + 4)).getInt();
            secondArray[i] = ByteBuffer.wrap(Arrays.copyOfRange(data, i * 14 + 4, i * 14 + 6)).getShort();
            lonArray[i] = ByteBuffer.wrap(Arrays.copyOfRange(data, i * 14 + 6, i * 14 + 10)).getFloat();
            latArray[i] = ByteBuffer.wrap(Arrays.copyOfRange(data, i * 14 + 10, (i + 1) * 14)).getFloat();
        }

        // int pos = 0;
        // try {
        //     DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        //     byte[] tem = new byte[14];
        //     while (in.read(tem) != -1) {
        //         idArray[pos]     = ByteBuffer.wrap(Arrays.copyOfRange(tem, 0, 4)).getInt();
        //         secondArray[pos] = ByteBuffer.wrap(Arrays.copyOfRange(tem, 4, 6)).getShort();
        //         lonArray[pos]    = ByteBuffer.wrap(Arrays.copyOfRange(tem, 6, 10)).getFloat();
        //         latArray[pos]    = ByteBuffer.wrap(Arrays.copyOfRange(tem, 10, 14)).getFloat();
        //         pos++;
        //     }
        //     in.close();
        // } catch (Exception e) {
        //     e.printStackTrace();
        // }
    }

    public static byte[] readByteArray(String readPath) {
        byte[] tem = new byte[245760];
        int length = 0;
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(readPath)));
            length = in.read(tem);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        byte[] finalByteArray = new byte[length];
        for (int i = 0; i < length; i++) {
            finalByteArray[i] = tem[i];
        }
        return finalByteArray;
    }

    public static byte[] unGzip(byte[] data) {
        byte[] b = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            GZIPInputStream gzip = new GZIPInputStream(bis);
            byte[] buf = new byte[1024];
            int num = -1;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((num = gzip.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, num);
            }
            b = baos.toByteArray();
            baos.flush();
            baos.close();
            gzip.close();
            bis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return b;
    }

    private static void writeDataToFile(String rootPath, int[][] idArray, short[][] secondArray, float[][] lonArray,
            float[][] latArray, int timePartitionsNum, int batchSize) {
        int numOfLines = idArray[0].length / timePartitionsNum;
        int pos = 0;
        while (pos < idArray[0].length) {
            try {
                int timeParSeq = pos / numOfLines;
                String path = rootPath + String.valueOf(timeParSeq) + ".tstjs";
                System.out.println(path);
                DataOutputStream out = new DataOutputStream(new FileOutputStream(path, true));
                ArrayList<Byte> byteData = new ArrayList<Byte>();
                for (int i = 0; i < batchSize; i++) {
                    int count = 0;
                    int temPos = pos;
                    while (count < numOfLines) {
                        int id = idArray[i][temPos];
                        short snap = secondArray[i][temPos];
                        float lon = lonArray[i][temPos];
                        float lat = latArray[i][temPos];
                        byte[] objectIDArray = intToByteArrayHigh(id);
                        byte[] temSnapArray = shortToByteArrayHigh(snap);
                        byte[] floatLonArray = floatToBytesArray(lon);
                        byte[] floatLatArray = floatToBytesArray(lat);
                        for (int j = 0; j < objectIDArray.length; j++) {
                            byteData.add(objectIDArray[j]);
                        }
                        for (int j = 0; j < temSnapArray.length; j++) {
                            byteData.add(temSnapArray[j]);
                        }
                        for (int j = 0; j < floatLonArray.length; j++) {
                            byteData.add(floatLonArray[j]);
                        }
                        for (int j = 0; j < floatLatArray.length; j++) {
                            byteData.add(floatLatArray[j]);
                        }
                        count += 1;
                        temPos++;
                    }
                }
                pos += numOfLines;
                byte[] byteArray = new byte[byteData.size()];
                for (int i = 0; i < byteArray.length; i++) {
                    byteArray[i] = byteData.get(i);
                }
                out.write(byteArray);
                out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static ArrayList<String> readTextFile(String path) {
        ArrayList<String> lines = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lines;
    }

    private static void getBinaryResult(ArrayList<String> originalData, int[] idArray, short[] secondArray,
            float[] lonArray, float[] latArray, String originalDate, String originalTime, String delimiter) {
        for (int i = 0; i < originalData.size(); i++) {
            String line = originalData.get(i);
            String[] lines = line.split(delimiter);
            if (lines.length != 5) {
                continue;
            }
            if (i == 0) {
                originalDate = lines[1];
                originalTime = lines[2];
            }
            idArray[i] = Integer.valueOf(lines[0]);
            secondArray[i] = convertTime(lines[1], lines[2], originalDate, originalTime);
            lonArray[i] = Float.valueOf(lines[3]);
            latArray[i] = Float.valueOf(lines[4]);
        }
    }

    private static short convertTime(String date, String time, String originalDate, String originaTime) {
        String[] dates = date.split("-");
        String[] times = time.split(":");
        String[] originalDates = originalDate.split("-");
        String[] originalTimes = originaTime.split(":");
        Calendar nowTime = Calendar.getInstance();
        Calendar originalTime = Calendar.getInstance();
        nowTime.set(Integer.valueOf(dates[0]), Integer.valueOf(dates[1]), Integer.valueOf(dates[2]),
                Integer.valueOf(times[0]), Integer.valueOf(times[1]), Integer.valueOf(times[2]));
        originalTime.set(Integer.valueOf(originalDates[0]), Integer.valueOf(originalDates[1]),
                Integer.valueOf(originalDates[2]), Integer.valueOf(originalTimes[0]), Integer.valueOf(originalTimes[1]),
                Integer.valueOf(originalTimes[2]));
        long nowSeconds = nowTime.getTimeInMillis() / 1000;
        long originalSeconds = originalTime.getTimeInMillis() / 1000;
        return (short) ((nowSeconds - originalSeconds) / 5);
    }

    public static byte[] intToByteArrayHigh(int num) {
        byte[] b = new byte[4];
        b[3] = (byte) (num & 0xff);
        b[2] = (byte) (num >> 8 & 0xff);
        b[1] = (byte) (num >> 16 & 0xff);
        b[0] = (byte) (num >> 24 & 0xff);
        return b;
    }

    public static byte[] shortToByteArrayHigh(short num) {
        byte[] b = new byte[2];
        b[1] = (byte) (num & 0xff);
        b[0] = (byte) (num >> 8 & 0xff);
        return b;
    }

    public static byte[] floatToBytesArray(float value) {
        int valueInt = Float.floatToRawIntBits(value);
        byte[] byteRet = new byte[4];
        for (int i = 0; i < 4; i++) {
            byteRet[3 - i] = (byte) ((valueInt >> 8 * i) & 0xff);
        }
        return byteRet;
    }


    
}