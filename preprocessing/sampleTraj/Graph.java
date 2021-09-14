package sampleTraj;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.PriorityQueue;
import java.util.Comparator;

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

import typeOfSigmod.Point;
import typeOfSigmod.Vertex;
import typeOfSigmod.Edge;

public class Graph{
    public HashMap<Integer, Vertex>  vertexsHashMap;
    public HashMap<Integer, Edge>    edgesHashMap;
    public HashMap<Integer, Integer> vertexMap;
    public int                       vertexNumber;
    
    public Graph(){
        this.vertexsHashMap = new HashMap<Integer, Vertex>();
        this.edgesHashMap   = new HashMap<Integer, Edge>();
        this.vertexMap      = new HashMap<Integer, Integer>();
        this.vertexNumber   = 0;
    }

    public void readVertexs(String nodePath){
        System.out.println("正在从文件中读取顶点信息...");
        Vertex temVertex;
        int count=0;
        try{
            BufferedReader reader = new BufferedReader(new FileReader(new File(nodePath)));
            String line;
            while((line = reader.readLine())!=null){
                String[] fields    = line.split("\t| ");
                int      ID        = Integer.valueOf(fields[0]);
                double   longitude = Double.valueOf(fields[2]);
                double   latitude  = Double.valueOf(fields[1]);
                temVertex = new Vertex(count, longitude, latitude);
                this.vertexsHashMap.put(count, temVertex);
                count++;
            }
            this.vertexNumber = count;
            reader.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("从文件中读取顶点信息完毕!!!------共读取" + count + "个顶点\n");
    }

    public void readEdges(String edgePath){
        System.out.println("正在从文件中读取边信息...");
        Edge temEdge;
        int count=0;
        try{
            BufferedReader reader = new BufferedReader(new FileReader(new File(edgePath)));
            String line;
            while((line = reader.readLine())!=null){
                String[] fields   = line.split("\t| ");
                int ID            = Integer.valueOf(fields[0]);
                int startVertexID = Integer.valueOf(fields[1]);
                int endVertexID   = Integer.valueOf(fields[2]);
                if ((this.vertexMap.containsKey(startVertexID) && (this.vertexMap.get(startVertexID) == endVertexID))
                        || (this.vertexMap.containsKey(endVertexID) && (this.vertexMap.get(endVertexID) == startVertexID))) {
                    continue;
                }
                temEdge = new Edge(count, this.vertexsHashMap.get(startVertexID), this.vertexsHashMap.get(endVertexID));
                this.vertexMap.put(startVertexID, endVertexID);
                this.edgesHashMap.put(count, temEdge);
                this.vertexsHashMap.get(startVertexID).vertexs.add(this.vertexsHashMap.get(endVertexID));
                this.vertexsHashMap.get(startVertexID).edges.add(temEdge);
                this.vertexsHashMap.get(endVertexID).vertexs.add(this.vertexsHashMap.get(startVertexID));
                this.vertexsHashMap.get(endVertexID).edges.add(temEdge);
                count++;
            }
            reader.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("从文件中读取边信息完毕!!!------共读取" + count + "条边\n");
    }

    public ArrayList<Edge> readRandomWalkPath(String filePath) {
        System.out.println("正在从文件中读取随机游走后的路径信息...");
        ArrayList<Edge> trajPath = new ArrayList<Edge>();
        try{
            BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
            String line;
            while((line = reader.readLine())!=null){
                String[] fields   = line.split("\t| |,");
                int ID            = Integer.valueOf(fields[0]);
                float travelTime  = Float.valueOf(fields[1]);
                this.edgesHashMap.get(ID).setTravelTime(travelTime);
                trajPath.add(this.edgesHashMap.get(ID));
            }
            reader.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return trajPath;
    }

    public int getStartVertex(ArrayList<Edge> trajPath){
        Edge firEdge = trajPath.get(0);
        Edge secEdge = trajPath.get(1);
        if ((firEdge.firstVertex.vertexID == secEdge.firstVertex.vertexID) || (firEdge.firstVertex.vertexID == secEdge.secVertex.vertexID)){
            return firEdge.firstVertex.vertexID;
        } else {
            return firEdge.secVertex.vertexID;
        }
    }




    public Edge findEdge(Vertex firstVertex, Vertex secVertex) {
        Edge tem = new Edge();
        for (int i = 0; i < firstVertex.edges.size(); i++) {
            tem = firstVertex.edges.get(i);
            if ((tem.firstVertex.equals(secVertex)) || (tem.secVertex.equals(secVertex))) {
                break;
            }
        }
        return tem;
    }

    private double eucLengthOfVertexs(Vertex temVertex, int ID) {
        double x1 = temVertex.longitude;
        double y1 = temVertex.latitude;
        double x2 = vertexsHashMap.get(ID).longitude;
        double y2 = vertexsHashMap.get(ID).latitude;
        double x  = x1 - x2;
        double y  = y1 - y2;
        return Math.sqrt(x * x + y * y);
    }

    public double totalLength(ArrayList<Edge> trajPath){
        double temLength = 0.0;
        for (int i = 0; i < trajPath.size(); i++) {
            temLength += trajPath.get(i).realDistance;
        }
        return temLength;
    }

    public void sampleTrajAccordRandomWalk(int beginTrajCount, int endTrajCount, String rootReadPath, String rootWritePath){
        for (int i = beginTrajCount; i < endTrajCount; i++) {
            String readPath = rootReadPath + String.valueOf(i) + ".txt";
            ArrayList<Edge> trajPath = readRandomWalkPath(readPath);
            int startVertex = getStartVertex(trajPath);
            ArrayList<Point> temPoint = disTrajPath(trajPath, startVertex, 0, -1);
            writeDataToFileInFormatGzip(temPoint, rootWritePath, i);
        }
        
    }

    public ArrayList<Point> disTrajPath(ArrayList<Edge> trajPath, int startVertexID, int endVertexID, int flag){
        System.out.println("开始采样...");
        Vertex helpFirst     = new Vertex();
        Vertex helpSec       = new Vertex();
        helpFirst.vertexID   = startVertexID;
        ArrayList<Point> temArrayPoint = new ArrayList<Point>();
        for (int i = 0; i < trajPath.size(); i++){
            Edge helpEdge = trajPath.get(i);
            double sectionLength = 0.0;
            if (flag == 0){
                sectionLength = 2;
            } else {
                double currEdgeLength = helpEdge.realDistance;
                double currEdgeTravelTime = helpEdge.travelTime;
                // 5 is sample rate
                sectionLength = currEdgeLength / currEdgeTravelTime * 5;
            }
            
            if (helpFirst.vertexID == helpEdge.firstVertexID) {
                helpFirst.longitude = helpEdge.firstVertex.longitude;
                helpFirst.latitude  = helpEdge.firstVertex.latitude;
                helpSec.longitude   = helpEdge.secVertex.longitude;
                helpSec.latitude    = helpEdge.secVertex.latitude;
                helpFirst.vertexID  = helpEdge.secVertexID;
            } else {
                helpFirst.longitude = helpEdge.secVertex.longitude;
                helpFirst.latitude  = helpEdge.secVertex.latitude;
                helpSec.longitude   = helpEdge.firstVertex.longitude;
                helpSec.latitude    = helpEdge.firstVertex.latitude;
                helpFirst.vertexID  = helpEdge.firstVertexID;
            }
            double x1      = helpFirst.longitude;
            double y1      = helpFirst.latitude;
            double x2      = helpSec.longitude;
            double y2      = helpSec.latitude;
            double xLength = (x2 - x1) * sectionLength / (helpEdge.realDistance);
            double yLength = (y2 - y1) * sectionLength / (helpEdge.realDistance);
            double x       = x1;
            double y       = y1;
            while(Edge.realDistance((x1 - x), (y1 - y), y) < helpEdge.realDistance){
                Point myPoint = new Point(x,y);
                temArrayPoint.add(myPoint);
                x += xLength;
                y += yLength;
            }
        }
        Point myPoint=new Point(helpSec.longitude, helpSec.latitude);
        temArrayPoint.add(myPoint);
        return temArrayPoint;
    }

    public boolean writeDataToFileInFormat(ArrayList<Point> arrayPoint, String path, int objectID){
        System.out.println("开始写文件...");
        int totalSnap = 17280;
        int nowSnap   = 0;
        try{
            // String trajPath = path + "trajectory" + String.valueOf(objectID) + ".tstjs";
            // DataOutputStream out = new DataOutputStream(new FileOutputStream(trajPath));
            // while(nowSnap < totalSnap){
            //     String nowTime = changeSnapToTime(nowSnap);
            //     double lon     = arrayPoint.get(nowSnap).longitude;
            //     double lat     = arrayPoint.get(nowSnap).latitude;
            //     out.writeInt(objectID);
            //     out.writeShort(convertTime("2020-06-01", nowTime, "2020-06-01", "00:00:00"));
            //     out.writeFloat((float)lon);
            //     out.writeFloat((float)lat);
            //     nowSnap++;
            // }
            // out.close();
            // System.out.println("写文件完毕...");

            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path)));
            while(nowSnap < totalSnap){
                String nowTime = changeSnapToTime(nowSnap);
                double lon     = arrayPoint.get(nowSnap).longitude;
                double lat     = arrayPoint.get(nowSnap).latitude;
                writer.write(objectID + "\t" + "2020-06-01" + "\t" + nowTime + "\t" + lon + "\t" + lat + "\n");
                nowSnap++;
            }
            writer.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        if (nowSnap == totalSnap){
            return true;
        }else{
            return false;
        }
    }

    public boolean writeDataToFileInFormatGzip(ArrayList<Point> arrayPoint, String path, int objectID) {
        System.out.println("开始写文件...");
        int totalSnap = 17280;
        int nowSnap = 0;
        try {
            String trajPath = path + "trajectory" + String.valueOf(objectID) + ".tstjs";
            DataOutputStream out = new DataOutputStream(new FileOutputStream(trajPath));
            ArrayList<Byte> byteData = new ArrayList<Byte>();
            while (nowSnap < totalSnap) {
                String nowTime = changeSnapToTime(nowSnap);
                double lon = arrayPoint.get(nowSnap).longitude;
                double lat = arrayPoint.get(nowSnap).latitude;
                short temSnap = convertTime("2020-06-01", nowTime, "2020-06-01", "00:00:00");
                float floatLon = (float) lon;
                float floatLat = (float) lat;
                byte[] objectIDArray = intToByteArrayHigh(objectID);
                byte[] temSnapArray = shortToByteArrayHigh(temSnap);
                byte[] floatLonArray = floatToBytesArray(floatLon);
                byte[] floatLatArray = floatToBytesArray(floatLat);
                for (int i = 0; i < objectIDArray.length; i++) {
                    byteData.add(objectIDArray[i]);
                }
                for (int i = 0; i < temSnapArray.length; i++) {
                    byteData.add(temSnapArray[i]);
                }
                for (int i = 0; i < floatLonArray.length; i++) {
                    byteData.add(floatLonArray[i]);
                }
                for (int i = 0; i < floatLatArray.length; i++) {
                    byteData.add(floatLatArray[i]);
                }
                nowSnap++;
            }
            byte[] beforeZipByteArray = new byte[byteData.size()];
            for (int i = 0; i < beforeZipByteArray.length; i++) {
                beforeZipByteArray[i] = byteData.get(i);
            }
            byte[] afterZipByteArray = gzip(beforeZipByteArray);
            out.write(afterZipByteArray);
            out.close();
            System.out.println("写文件完毕...");

            // BufferedWriter writer = new BufferedWriter(new FileWriter(new
            // File(trajPath)));
            // while(nowSnap < totalSnap){
            // String nowTime = changeSnapToTime(nowSnap);
            // double lon = arrayPoint.get(nowSnap).longitude;
            // double lat = arrayPoint.get(nowSnap).latitude;
            // writer.write(objectID + "\t" + "2020-06-01" + "\t" + nowTime + "\t" + lon +
            // "\t" + lat + "\n");
            // nowSnap++;
            // }
            // writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (nowSnap == totalSnap) {
            return true;
        } else {
            return false;
        }
    }

    public static String changeSnapToTime(int snapshot){
        int totalSecond = snapshot * 5;
        int hour        = totalSecond / 3600;
        totalSecond     = totalSecond % 3600;
        int minute      = totalSecond / 60;
        totalSecond     = totalSecond % 60;
        int second      = totalSecond;
        String hourStr;
        String minuteStr;
        String secondStr;
        if (hour <= 9){
            hourStr = "0" + String.valueOf(hour);
        }else{
            hourStr = String.valueOf(hour);
        }
        if (minute <= 9){
            minuteStr = "0" + String.valueOf(minute);
        }else{
            minuteStr = String.valueOf(minute);
        }
        if (second <= 9){
            secondStr = "0" + String.valueOf(second);
        }else{
            secondStr = String.valueOf(second);
        }
        return hourStr + ":" + minuteStr + ":" + secondStr;
    }

    public void resetAccessFlag(){
        for (int i = 0; i < vertexNumber; i++){
            vertexsHashMap.get(i).accessFlag = false;
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

    public static byte[] gzip(byte[] data) {
        byte[] b = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(bos);
            gzip.write(data);
            gzip.finish();
            gzip.close();
            b = bos.toByteArray();
            bos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return b;
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

    public static void testResult(String path) {
        byte[] readData = readByteArray(path);
        byte[] data = unGzip(readData);
        for (int i = 0; i < 17280; i++) {
            int id = ByteBuffer.wrap(Arrays.copyOfRange(data, i * 14, i * 14 + 4)).getInt();
            short snap = ByteBuffer.wrap(Arrays.copyOfRange(data, i * 14 + 4, i * 14 + 6)).getShort();
            float lon = ByteBuffer.wrap(Arrays.copyOfRange(data, i * 14 + 6, i * 14 + 10)).getFloat();
            float lat = ByteBuffer.wrap(Arrays.copyOfRange(data, i * 14 + 10, (i + 1) * 14)).getFloat();
            System.out.println(id + "\t" + snap + "\t" + lon + "\t" + lat);
        }
    }


}