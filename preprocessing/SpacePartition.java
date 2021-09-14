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


public class SpacePartition {
    public static void main(String args[]) {
        String readPath      = args[0];
        String writePath     = args[1];
        int timePars         = Integer.valueOf(args[2]);
        int totalTrajNums    = Integer.valueOf(args[3]);
        int spacePars        = Integer.valueOf(args[4]);
        int beginTimeParsNum = Integer.valueOf(args[5]);
        int sliceLength      = Integer.valueOf(args[6]);
        textRedistribu(readPath, writePath, timePars, totalTrajNums, 360, spacePars, "zorder", "_map",
                beginTimeParsNum, sliceLength);
    }

    public static void textRedistribu(String rootReadPath, String rootWritePath, int timeParNum, int trajNum,
            int filterNum, int spaceParNum, String prefix, String prefix2, int beginTimeParsNum, int sliceLength) {
        for (int i = beginTimeParsNum; i < beginTimeParsNum + 1; i++) {
            String path = rootReadPath.concat(String.valueOf(i)).concat(".tstjs");
            System.out.println(path);
            long startTime = System.nanoTime(), endTime = 0;
            double[][] coordinates = readTrajData2(path, trajNum, filterNum);
            endTime = System.nanoTime();
            startTime = endTime;

            int[] idArray = new int[trajNum * filterNum];
            short[] secondArray = new short[trajNum * filterNum];
            float[] lonArray = new float[trajNum * filterNum];
            float[] latArray = new float[trajNum * filterNum];
            readTrajDataInFormat(path, idArray, secondArray, lonArray, latArray);
            endTime = System.nanoTime();
            startTime = endTime;
            int[][] zValue = zorder(coordinates, trajNum);
            endTime = System.nanoTime();
            startTime = endTime;
            Arrays.sort(zValue, new SortComparator());

            endTime = System.nanoTime();
            startTime = endTime;
            int[][] temMap = new int[trajNum][3];
            for (int j = 0; j < spaceParNum; j++) {
                int[] idList;
                if ((trajNum - j * sliceLength) >= sliceLength) {
                    idList = new int[sliceLength];
                } else {
                    idList = new int[(trajNum - j * sliceLength)];
                }
                int sliceLengthPos = 0;
                for (int k = 0; k < sliceLength; k++) {
                    if ((sliceLength * j + k) == zValue.length) {
                        break;
                    }
                    idList[k] = zValue[sliceLength * j + k][0];
                    temMap[sliceLength * j + k][0] = zValue[sliceLength * j + k][0];
                    temMap[sliceLength * j + k][1] = j;
                    temMap[sliceLength * j + k][2] = k;
                    sliceLengthPos += 1;
                }
                int[] idArrayTem = new int[sliceLengthPos * filterNum];
                short[] secondArrayTem = new short[sliceLengthPos * filterNum];
                float[] lonArrayTem = new float[sliceLengthPos * filterNum];
                float[] latArrayTem = new float[sliceLengthPos * filterNum];
                getLocalData(idList, idArray, secondArray, lonArray, latArray, idArrayTem, secondArrayTem, lonArrayTem,
                        latArrayTem, sliceLengthPos * filterNum, filterNum);

                endTime = System.nanoTime();
                startTime = endTime;
                path = rootWritePath + "_ori" + String.valueOf(i) + prefix + String.valueOf(j) + ".tstjs";
                writeTrajDataInFormat(path, idArrayTem, secondArrayTem, lonArrayTem, latArrayTem);
                endTime = System.nanoTime();
                startTime = endTime;
            }
            String binaryPath = rootWritePath + prefix2 + String.valueOf(i) + ".tstjs";
            String txtPath = rootWritePath + prefix2 + String.valueOf(i) + ".txt";
            Arrays.sort(temMap, new SortTmpComparator());
            writeMapToFile(binaryPath, txtPath, temMap);
        }
    }

    private static double[][] readTrajData2(String path, int totalSnap, int filterNum) {
        double[][] coordinates = new double[totalSnap][2];
        int pos = 0;
        int count = 0;
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
            byte[] tem = new byte[14];
            while (in.read(tem) != -1) {
                if (count % filterNum == 0) {
                    coordinates[pos][0] = (double) ByteBuffer.wrap(Arrays.copyOfRange(tem, 6, 10)).getFloat();
                    coordinates[pos][1] = (double) ByteBuffer.wrap(Arrays.copyOfRange(tem, 10, 14)).getFloat();
                    pos++;
                    if (pos == totalSnap) {
                        break;
                    }
                }
                count++;
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return coordinates;
    }

    private static void readTrajDataInFormat(String path, int[] idArray, short[] secondArray, float[] lonArray,
            float[] latArray) {
        int pos = 0;
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
            byte[] tem = new byte[14];
            while (in.read(tem) != -1) {
                idArray[pos] = ByteBuffer.wrap(Arrays.copyOfRange(tem, 0, 4)).getInt();
                secondArray[pos] = ByteBuffer.wrap(Arrays.copyOfRange(tem, 4, 6)).getShort();
                lonArray[pos] = ByteBuffer.wrap(Arrays.copyOfRange(tem, 6, 10)).getFloat();
                latArray[pos] = ByteBuffer.wrap(Arrays.copyOfRange(tem, 10, 14)).getFloat();
                pos++;
                if (pos == idArray.length) {
                    break;
                }
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int[][] zorder(double coordinate[][], int length) {
        int gridLength = 100;
        double minLon = Double.MAX_VALUE;
        double maxLon = Double.MIN_VALUE;
        double minLat = Double.MAX_VALUE;
        double maxLat = Double.MIN_VALUE;
        for (int i = 0; i < length; i++) {
            minLon = Math.min(minLon, coordinate[i][0]);
            maxLon = Math.max(maxLon, coordinate[i][0]);
            minLat = Math.min(minLat, coordinate[i][1]);
            maxLat = Math.max(maxLat, coordinate[i][1]);
        }
        double lonDistance1 = RealDistance.realDistance(maxLon, maxLat, minLon, maxLat);
        double lonDistance2 = RealDistance.realDistance(maxLon, minLat, minLon, minLat);
        double latDistance1 = RealDistance.realDistance(maxLon, maxLat, maxLon, minLat);
        double latDistance2 = RealDistance.realDistance(minLon, maxLat, minLon, minLat);
        double setDistance = maxFourValue(lonDistance1, lonDistance2, latDistance1, latDistance2);
        int gridNums = (int) Math.ceil(setDistance / gridLength);
        int order = 1;
        while (true) {
            if (Math.pow(2, order) > gridNums) {
                break;
            }
            order++;
        }
        gridNums = (int) Math.pow(2, order);
        gridLength = (int) (setDistance / gridNums);
        double lonGridLen = (maxLon - minLon + 0.001) / gridNums;
        double latGridLen = (maxLat - minLat + 0.001) / gridNums;
        int[][] num = new int[length][2];
        for (int i = 0; i < length; i++) {
            num[i][0] = (int) Math.floor((coordinate[i][0] - minLon) / lonGridLen);
            num[i][1] = (int) Math.floor((coordinate[i][1] - minLat) / latGridLen);
        }
        int[][] value = new int[length][2];
        for (int coorPos = 0; coorPos < length; coorPos++) {
            value[coorPos][0] = coorPos;
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < order; j++) {
                    int mask = 1 << j;
                    if ((num[coorPos][1 - i] & mask) != 0) {
                        value[coorPos][1] |= 1 << (i + j * 2);
                    }
                }
            }
        }
        return value;
    }

    private static double maxFourValue(final double a1, final double a2, final double a3, final double a4) {
        double b1 = Math.max(a1, a2);
        double b2 = Math.max(a3, a4);
        return Math.max(b1, b2);
    }

    private static void getLocalData(int[] idList, int[] idArray, short[] secondArray, float[] lonArray,
            float[] latArray, int[] idArrayTem, short[] secondArrayTem, float[] lonArrayTem, float[] latArrayTem,
            int arrayLength, int filterNum) {
        int totalPos = 0;
        for (int id : idList) {
            int beginPos = filterNum * id;
            for (int i = 0; i < filterNum; i++) {
                idArrayTem[totalPos] = idArray[beginPos + i];
                secondArrayTem[totalPos] = secondArray[beginPos + i];
                lonArrayTem[totalPos] = lonArray[beginPos + i];
                latArrayTem[totalPos] = latArray[beginPos + i];
                totalPos++;
            }
        }
    }

    private static void writeTrajDataInFormat(String path, int[] idArray, short[] secondArray, float[] lonArray,
            float[] latArray) {
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
            ArrayList<Byte> byteData = new ArrayList<Byte>();
            for (int i = 0; i < idArray.length; i++) {
                // out.writeInt(idArray[i]);
                // out.writeShort(secondArray[i]);
                // out.writeFloat(lonArray[i]);
                // out.writeFloat(latArray[i]);
                byte[] floatLonArray = floatToBytesArray(lonArray[i]);
                byte[] floatLatArray = floatToBytesArray(latArray[i]);
                for (int j = 0; j < floatLonArray.length; j++) {
                    byteData.add(floatLonArray[j]);
                }
                for (int j = 0; j < floatLatArray.length; j++) {
                    byteData.add(floatLatArray[j]);
                }
            }
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

    private static void writeMapToFile(String binaryPath, String txtPath, int[][] mapData) {
        try {
            int count = 0;
            DataOutputStream out = new DataOutputStream(new FileOutputStream(binaryPath));
            while (count < mapData.length) {
                out.writeByte((byte) mapData[count][1]);
                out.writeShort((short) mapData[count][2]);
                count += 1;
            }
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(txtPath)));
            for (int i = 0; i < mapData.length; i++) {
                writer.write(mapData[i][0] + "\t" + mapData[i][1] + "\t" + mapData[i][2] + "\n");
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
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


class SortComparator implements Comparator<int[]> {
    // Used for sorting in ascending order of
    // roll number
    public int compare(int[] a, int[] b) {
        return a[1] - b[1];
    }
}

class SortTmpComparator implements Comparator<int[]> {
    // Used for sorting in ascending order of
    // roll number
    public int compare(int[] a, int[] b) {
        return a[0] - b[0];
    }
}