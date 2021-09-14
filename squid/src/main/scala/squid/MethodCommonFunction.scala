package src.main.scala.squid

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel

import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
import scala.collection.mutable.Set
import scala.collection.mutable.Map
import scala.io.Source


import java.nio.ByteBuffer
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.util.ArrayList
import java.util.Arrays
import java.net.InetAddress

import src.main.scala.dataFormat.BaseSetting
import src.main.scala.dataFormat.MBR
import src.main.scala.index.InvertedIndex
import src.main.scala.util.PublicFunc
import src.main.scala.util.CompressMethod
import src.main.scala.util.trajic.GPSPoint

object MethodCommonFunction {

    def getInitialInputFilePathWhole(myBaseSettings : BaseSetting, prefix1 : String,
                                prefix2 : String, prefix3 : String, prefix4 : String, prefix5 : String) : Array[String] = {
        val daysNum            : Int = myBaseSettings.daysNum
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        var finalPath : Array[String] = new Array[String](daysNum * timePartitionsNum * spacePartitionsNum)
        for (i <- 0 until daysNum) {
            for (j <- 0 until timePartitionsNum) {
                for (k <- 0 until spacePartitionsNum) {
                    // 用于billion数据集的join
                    // val pathName : String = myBaseSettings.rootPath.concat(prefix1).concat((i).toString).concat(prefix2).concat((i).toString).concat(prefix3).concat(j.toString)
                    //                                   .concat(prefix4).concat(k.toString).concat(prefix5)
                    // 用于trallion数据集的query
                    val pathName : String = myBaseSettings.rootPath.concat(prefix2).concat((i).toString).concat(prefix3).concat(j.toString)
                                                      .concat(prefix4).concat(k.toString).concat(prefix5)
                    finalPath(i * timePartitionsNum * spacePartitionsNum + j * spacePartitionsNum + k) = pathName
                }
            }
        }
        finalPath
    }

    def getLookupTableFilePathWhole(myBaseSettings : BaseSetting, prefix1 : String,
                                prefix2 : String, prefix3 : String, prefix4 : String) : Array[String] = {
        val daysNum           : Int = myBaseSettings.daysNum
        val timePartitionsNum : Int = myBaseSettings.timePartitionsNum
        var finalPath : Array[String] = new Array[String](daysNum * timePartitionsNum)
        for (i <- 0 until daysNum) {
            for (j <- 0 until timePartitionsNum){
                // 用于billion数据集的join
                // finalPath(i * timePartitionsNum + j) = myBaseSettings.rootPath.concat(prefix1).concat(i.toString).concat(prefix2).concat(i.toString).concat(prefix3)
                //                                             .concat(j.toString).concat(prefix4)
                // 用于trillion数据集的query
                finalPath(i * timePartitionsNum + j) = myBaseSettings.rootPath.concat(prefix2).concat(i.toString).concat(prefix3)
                                                            .concat(j.toString).concat(prefix4)
            }
        }
        finalPath
    }

    def getLookupTableWhole(sc : SparkContext, lookupTableFilePath : Array[String],
                        myBaseSettings : BaseSetting) : Array[Array[Int]] = {
        val daysNum            : Int = myBaseSettings.daysNum
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val totalTrajNums      : Int = myBaseSettings.totalTrajNums
        val trajNumEachSpace   : Int = myBaseSettings.trajNumEachSpace

        val inputByte : Array[Array[Array[Byte]]] = sc.binaryRecords(lookupTableFilePath.mkString(","), 3)
                                                      .glom()
                                                      .collect()
        var lookupTable : Array[Array[Int]] = Array.ofDim[Int](daysNum, timePartitionsNum * totalTrajNums * 3)
        for (i <- 0 until inputByte.length) {
            for (j <- 0 until inputByte(i).length) {
                val line : Array[Byte] = inputByte(i)(j)
                var spaceID : Int = ByteBuffer.wrap(line.slice(0, 1)).get.toInt
                if (spaceID < 0) {
                    spaceID = spaceID + 256
                }
                var spaceOrder : Int = ByteBuffer.wrap(line.slice(1, 3)).getShort.toInt
                if (spaceOrder < 0) {
                    spaceOrder = spaceOrder + 65536
                }
                val dayPos : Int = i / timePartitionsNum
                val firPos : Int = (i % timePartitionsNum) * totalTrajNums * 3 + j * 3
                lookupTable(dayPos)(firPos)     = spaceID
                lookupTable(dayPos)(firPos + 1) = spaceOrder
                val secPos : Int = (i % timePartitionsNum) * totalTrajNums * 3 + spaceID * trajNumEachSpace * 3 + spaceOrder * 3 + 2
                lookupTable(dayPos)(secPos)     = j
                
            }
        }
        lookupTable
    }

    def setIndexUsingGzipWhole(sc : SparkContext, inputFilePath : Array[String], myBaseSettings : BaseSetting, 
                    bcLookupTable : Broadcast[Array[Array[Int]]]) : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = {
        val inputRDD1 = sc.binaryFiles(inputFilePath.mkString(","))
        val inputRDD : RDD[(String, Array[Array[Byte]])] = inputRDD1.map(l => (l._1, l._2.toArray))
                                                                    .map(l => (l._1, CompressMethod.unGzip(l._2)))
                                                                    .map(l => (l._1, l._2.grouped(8).toArray.toArray))

        val indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = inputRDD.map(l => mapSpaceParToInvertedIndexAndRangeQueryWhole(l, myBaseSettings, bcLookupTable))
                                                                                   .persist(StorageLevel.MEMORY_ONLY_SER)
        val time1 : Long = System.currentTimeMillis
        println(indexRDD.count())
        val time2 : Long = System.currentTimeMillis
        println("Index build time: " + ((time2 - time1) / 1000.0).toDouble)
        indexRDD
    }

    def setIndexFromFile(sc : SparkContext, myBaseSettings : BaseSetting) : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = {
        val daysNum            : Int = myBaseSettings.daysNum
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        var inputFilePath : Array[String] = new Array[String](daysNum * timePartitionsNum * spacePartitionsNum)
        for (i <- 0 until daysNum) {
            for (j <- 0 until timePartitionsNum) {
                for (k <- 0 until spacePartitionsNum) {
                    // 用于billion数据集的join
                    val pathName : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + myBaseSettings.trajNumEachSpace.toString + "day" + i.toString + "/day" + i.toString + "_index_" + (1440 / myBaseSettings.timePartitionsNum).toString + "/time" + j.toString + "space" + k.toString + ".txt"
                    // 用于trillion数据集的query
                    // val pathName : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + "/day" + i.toString + "_index/time" + j.toString + "space" + k.toString + ".txt"
                    inputFilePath(i * timePartitionsNum * spacePartitionsNum + j * spacePartitionsNum + k) = pathName
                }
            }
        }
        val inputRDD1 = sc.binaryFiles(inputFilePath.mkString(","))
        val indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = inputRDD1.map(l => (l._1, l._2.toArray))
                                                                                         .map(l => (parseFile(l._1), readIndexFromFile(l._2)))
                                                                                         .map(l => (l._1._1, l._1._2, l._1._3, l._2._1, l._2._2))
                                                                                         .persist(StorageLevel.MEMORY_ONLY)
        val firstRecord : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]) = indexRDD.first()
        val myIndex = firstRecord._5(0)._2
        val time1 : Long = System.currentTimeMillis
        println(indexRDD.count())
        val time2 : Long = System.currentTimeMillis
        println("Index read time: " + ((time2 - time1) / 1000.0).toDouble)
        indexRDD
    }

    def mapSpaceParToInvertedIndexAndRangeQueryWhole(iter1 : (String, Array[Array[Byte]]), myBaseSettings : BaseSetting, 
                                    bcLookupTable : Broadcast[Array[Array[Int]]]) : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]) = {
        val lookupTable        : Array[Array[Int]] = bcLookupTable.value
        val timePartitionsNum  : Int = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int = myBaseSettings.spacePartitionsNum
        val totalTrajNums      : Int = myBaseSettings.totalTrajNums
        val trajNumEachSpace   : Int = myBaseSettings.trajNumEachSpace
        val indexSnapInterval  : Int = myBaseSettings.indexSnapInterval
        val oneParSnapNum      : Int = myBaseSettings.oneParSnapNum
        
        val MINLON : Float = myBaseSettings.MINLON
        val MINLAT : Float = myBaseSettings.MINLAT
        val MAXLON : Float = myBaseSettings.MAXLON
        val MAXLAT : Float = myBaseSettings.MAXLAT

        var PARMINLON : Float = Float.MaxValue
        var PARMINLAT : Float = Float.MaxValue
        var PARMAXLON : Float = Float.MinValue
        var PARMAXLAT : Float = Float.MinValue
        
        val lonGridNum    : Int = myBaseSettings.lonGridNum
        val latGridNum    : Int = myBaseSettings.latGridNum
        var lonGridLength : Float = 0.0f
        var latGridLength : Float = 0.0f
        
        // Parse out dayID、timeID、spaceID
        val fileName : String = iter1._1
        var dayID    : Int = 0
        var timeID   : Int = 0
        var spaceID  : Int = 0
        val lines1 : Array[String] = fileName.split("/")
        val lines2 : Array[String] = lines1(lines1.length - 1).split("\\.")
        val line : String = lines2(0);
        if (line.charAt(8) == 'z'){
            var firStr = line.slice(7, 8)
            var secStr = line.slice(14, line.length)
            timeID = firStr.toInt
            spaceID = secStr.toInt
        }else if (line.charAt(9) == 'z'){
            var firStr = line.slice(7, 9)
            var secStr = line.slice(15, line.length)
            timeID = firStr.toInt
            spaceID = secStr.toInt
        } else {
            var firStr = line.slice(7, 10)
            var secStr = line.slice(16, line.length)
            timeID = firStr.toInt
            spaceID = secStr.toInt
        }
        // 用于billion数据集的join
        val lines3 : String = lines1(lines1.length - 3)
        // 用于trillion数据集的query
        // val lines3 : String = lines1(lines1.length - 2)
        dayID = lines3.slice(3, 4).toInt

        var mbrIndex : Array[MBR] = new Array[MBR](oneParSnapNum / indexSnapInterval + 1)
        for (i <- 0 until (oneParSnapNum / indexSnapInterval + 1)) {
            var mbr : MBR = new MBR(Float.MaxValue, Float.MinValue, Float.MaxValue, Float.MinValue)
            mbrIndex(i) = mbr
        }

        var temIndex : Array[Map[Int, ArrayBuffer[Int]]] = new Array[Map[Int, ArrayBuffer[Int]]](oneParSnapNum / indexSnapInterval + 1)
        
        var temPos : Int = 0
        var parSeq : Int = 0
        val iterArray : Array[Array[Byte]] = iter1._2
        for (record <- iterArray){
            if (temPos % oneParSnapNum == 0){
                parSeq = temPos / oneParSnapNum
            }
            if ((temPos % oneParSnapNum) % indexSnapInterval == 0){
                val timePos : Int = (temPos % oneParSnapNum) / indexSnapInterval
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                if (lon >= MINLON && lon <= MAXLON && lat >= MINLAT && lat <= MAXLAT){
                    var mbr : MBR = mbrIndex(timePos)
                    mbr.minLon = math.min(mbr.minLon, lon)
                    mbr.maxLon = math.max(mbr.maxLon, lon)
                    mbr.minLat = math.min(mbr.minLat, lat)
                    mbr.maxLat = math.max(mbr.maxLat, lat)
                    PARMINLON = math.min(PARMINLON, lon)
                    PARMAXLON = math.max(PARMAXLON, lon)
                    PARMINLAT = math.min(PARMINLAT, lat)
                    PARMAXLAT = math.max(PARMAXLAT, lat)
                    mbrIndex(timePos) = mbr
                }
            }
            if ((temPos + 1) % oneParSnapNum == 0){
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                if (lon >= MINLON && lon <= MAXLON && lat >= MINLAT && lat <= MAXLAT){
                    var mbr : MBR = mbrIndex(oneParSnapNum / indexSnapInterval)
                    mbr.minLon = math.min(mbr.minLon, lon)
                    mbr.maxLon = math.max(mbr.maxLon, lon)
                    mbr.minLat = math.min(mbr.minLat, lat)
                    mbr.maxLat = math.max(mbr.maxLat, lat)
                    PARMINLON = math.min(PARMINLON, lon)
                    PARMAXLON = math.max(PARMAXLON, lon)
                    PARMINLAT = math.min(PARMINLAT, lat)
                    PARMAXLAT = math.max(PARMAXLAT, lat)
                    mbrIndex(oneParSnapNum / indexSnapInterval) = mbr
                }
            }
            temPos += 1
        }

        temPos = 0
        parSeq = 0
        for (record <- iterArray){
            if (temPos % oneParSnapNum == 0){
                parSeq = temPos / oneParSnapNum
            }
            if ((temPos % oneParSnapNum) % indexSnapInterval == 0){
                val timePos : Int = (temPos % oneParSnapNum) / indexSnapInterval
                var mbr : MBR = mbrIndex(timePos)
                lonGridLength = (mbr.maxLon - mbr.minLon) / lonGridNum
                latGridLength = (mbr.maxLat - mbr.minLat) / latGridNum
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                if (lon >= MINLON && lon <= MAXLON && lat >= MINLAT && lat <= MAXLAT){
                    val lonGridID : Int = floor((lon - mbr.minLon) / lonGridLength).toInt
                    val latGridID : Int = floor((lat - mbr.minLat) / latGridLength).toInt
                    val gridID : Int = latGridID * lonGridNum + lonGridID
                    val trajID : Int = lookupTable(dayID)(timeID * totalTrajNums * 3 + spaceID * trajNumEachSpace * 3 + (temPos / oneParSnapNum) * 3 + 2)
                    val myOrderPos : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + trajID * 3 + 1)
                    if (temIndex(timePos) == null){
                        var temArray : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
                        var temArrayBuffer : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                        // temArrayBuffer += trajID
                        temArrayBuffer += myOrderPos
                        temArray += gridID -> temArrayBuffer
                        temIndex(timePos) = temArray
                    }else{
                        var temArray : Map[Int, ArrayBuffer[Int]] = temIndex(timePos)
                        if (temArray.contains(gridID)) {
                            var temArrayBuffer : ArrayBuffer[Int] = temArray.get(gridID).get
                            // temArrayBuffer += trajID
                            temArrayBuffer += myOrderPos
                            temArray += gridID -> temArrayBuffer
                        } else {
                            var temArrayBuffer : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                            // temArrayBuffer += trajID
                            temArrayBuffer += myOrderPos
                            temArray += gridID -> temArrayBuffer
                        }
                        temIndex(timePos) = temArray
                    }
                }
            }
            if ((temPos + 1) % oneParSnapNum == 0){
                var mbr : MBR = mbrIndex(oneParSnapNum / indexSnapInterval)
                lonGridLength = (mbr.maxLon - mbr.minLon) / lonGridNum
                latGridLength = (mbr.maxLat - mbr.minLat) / latGridNum
                val lon : Float = ByteBuffer.wrap(record.slice(0, 4)).getFloat
                val lat : Float = ByteBuffer.wrap(record.slice(4, 8)).getFloat
                if (lon >= MINLON && lon <= MAXLON && lat >= MINLAT && lat <= MAXLAT){
                    val lonGridID : Int = floor((lon - mbr.minLon) / lonGridLength).toInt
                    val latGridID : Int = floor((lat - mbr.minLat) / latGridLength).toInt
                    val gridID : Int = latGridID * lonGridNum + lonGridID
                    val trajID : Int = lookupTable(dayID)(timeID * totalTrajNums * 3 + spaceID * trajNumEachSpace * 3 + (temPos / oneParSnapNum) * 3 + 2)
                    val myOrderPos : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + trajID * 3 + 1)
                    if (temIndex(oneParSnapNum / indexSnapInterval) == null){
                        var temArray : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
                        var temArrayBuffer : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                        // temArrayBuffer += trajID
                        temArrayBuffer += myOrderPos
                        temArray += gridID -> temArrayBuffer
                        temIndex(oneParSnapNum / indexSnapInterval) = temArray
                    }else{
                        var temArray : Map[Int, ArrayBuffer[Int]] = temIndex(oneParSnapNum / indexSnapInterval)
                        if (temArray.contains(gridID)) {
                            var temArrayBuffer : ArrayBuffer[Int] = temArray.get(gridID).get
                            // temArrayBuffer += trajID
                            temArrayBuffer += myOrderPos
                            temArray += gridID -> temArrayBuffer
                        } else {
                            var temArrayBuffer : ArrayBuffer[Int] = new ArrayBuffer[Int]()
                            // temArrayBuffer += trajID
                            temArrayBuffer += myOrderPos
                            temArray += gridID -> temArrayBuffer
                        }
                        temIndex(oneParSnapNum / indexSnapInterval) = temArray
                    }
                }
            }
            temPos += 1
        }

        var invertedIndex : Array[InvertedIndex] = new Array[InvertedIndex](oneParSnapNum / indexSnapInterval + 1)
        for (i <- 0 until invertedIndex.length){
            var instanceIndex : InvertedIndex = new InvertedIndex(myBaseSettings.trajNumEachSpace, lonGridNum * latGridNum)
            for (j <- 0 until (lonGridNum * latGridNum)){
                var temArray : ArrayBuffer[Int] = ArrayBuffer[Int]()
                val temMap : Map[Int, ArrayBuffer[Int]] = temIndex(i)
                if (temMap != null && temMap.contains(j)) {
                    temArray = temMap.get(j).get
                }
                if (j == 0){
                    instanceIndex.index(0) = 0
                    instanceIndex.index(1) = temArray.length
                    instanceIndex.indexPos = 2
                    for (k <- 0 until temArray.length){
                        instanceIndex.idArray(k) = temArray(k)
                    }
                    instanceIndex.idArrayPos = temArray.length
                }else{
                    var temPos = instanceIndex.indexPos
                    instanceIndex.index(temPos) = instanceIndex.index(temPos - 2) + instanceIndex.index(temPos - 1)
                    instanceIndex.index(temPos + 1) = temArray.length
                    instanceIndex.indexPos += 2
                    temPos = instanceIndex.idArrayPos
                    for (k <- 0 until temArray.length){
                        instanceIndex.idArray(temPos) = temArray(k)
                        temPos += 1
                    }
                    instanceIndex.idArrayPos = temPos
                }
            }
            invertedIndex(i) = instanceIndex
        }

        var returnResult : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]) = (dayID, timeID, spaceID, new MBR(PARMINLON, PARMAXLON, PARMINLAT, PARMAXLAT), new Array[(MBR, InvertedIndex)](oneParSnapNum / indexSnapInterval + 1))
        for (i <- 0 until returnResult._5.length){
            returnResult._5(i) = (mbrIndex(i), invertedIndex(i))
        }
        
        // paramater change
        if (myBaseSettings.writeIndexToFileFlag == 1) {
            // 用于billion数据集的join
            val path : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + myBaseSettings.trajNumEachSpace.toString + "day" + (dayID).toString + "/day" + (dayID).toString + "_index_" + (1440 / myBaseSettings.timePartitionsNum).toString + "/time" + timeID.toString + "space" + spaceID.toString + ".txt"
            // 用于trillion数据集的query
            // val path : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + "/day" + (dayID).toString + "_index/time" + timeID.toString + "space" + spaceID.toString + ".txt"
            
            writeIndexToFileBinary(path, returnResult)
        }
        returnResult
    }

    def writeIndexToFileBinary(path : String, index : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])) : Unit = {
        var writer : DataOutputStream = new DataOutputStream(new FileOutputStream(path))
        val myMBR : MBR = index._4
        val myIndexArray : Array[(MBR, InvertedIndex)] = index._5
        writer.writeFloat(myMBR.minLon)
        writer.writeFloat(myMBR.maxLon)
        writer.writeFloat(myMBR.minLat)
        writer.writeFloat(myMBR.maxLat)
        writer.writeInt(myIndexArray.length)
        
        for (i <- 0 until myIndexArray.length) {
            val temMBR : MBR = myIndexArray(i)._1
            val temInvertedIndex : InvertedIndex = myIndexArray(i)._2
            writer.writeFloat(temMBR.minLon)
            writer.writeFloat(temMBR.maxLon)
            writer.writeFloat(temMBR.minLat)
            writer.writeFloat(temMBR.maxLat)
            writer.writeInt(temInvertedIndex.idArray.length)
            writer.writeInt(temInvertedIndex.index.length)
            for (j <- 0 until temInvertedIndex.idArray.length) {
                writer.writeInt(temInvertedIndex.idArray(j))
            }
            for (j <- 0 until temInvertedIndex.index.length) {
                writer.writeInt(temInvertedIndex.index(j))
            }
        }
        writer.close();
    }

    def readIndexFromFile(iter : Array[Byte]) : (MBR, Array[(MBR, InvertedIndex)]) = {
        var in : DataInputStream = new DataInputStream(new ByteArrayInputStream(iter))
        var tem : Array[Byte] = new Array[Byte](20)
        in.read(tem)
        var minLon : Float = ByteBuffer.wrap(Arrays.copyOfRange(tem, 0, 4)).getFloat();
        var maxLon : Float = ByteBuffer.wrap(Arrays.copyOfRange(tem, 4, 8)).getFloat();
        var minLat : Float = ByteBuffer.wrap(Arrays.copyOfRange(tem, 8, 12)).getFloat();
        var maxLat : Float = ByteBuffer.wrap(Arrays.copyOfRange(tem, 12, 16)).getFloat();
        var length : Int = ByteBuffer.wrap(Arrays.copyOfRange(tem, 16, 20)).getInt();
        var myMBR : MBR = new MBR(minLon, maxLon, minLat, maxLat)
        var myArray : Array[(MBR, InvertedIndex)] = new Array[(MBR, InvertedIndex)](length)

        for (i <- 0 until length) {
            var tem1 : Array[Byte] = new Array[Byte](24)
            in.read(tem1)
            minLon = ByteBuffer.wrap(Arrays.copyOfRange(tem1, 0, 4)).getFloat();
            maxLon = ByteBuffer.wrap(Arrays.copyOfRange(tem1, 4, 8)).getFloat();
            minLat = ByteBuffer.wrap(Arrays.copyOfRange(tem1, 8, 12)).getFloat();
            maxLat = ByteBuffer.wrap(Arrays.copyOfRange(tem1, 12, 16)).getFloat();
            var length1 : Int = ByteBuffer.wrap(Arrays.copyOfRange(tem1, 16, 20)).getInt();
            var length2 : Int = ByteBuffer.wrap(Arrays.copyOfRange(tem1, 20, 24)).getInt();
            var temMBR : MBR = new MBR(minLon, maxLon, minLat, maxLat)
            val temInvertedIndex : InvertedIndex = new InvertedIndex(length1, length2 / 2)
            var tem2 : Array[Byte] = new Array[Byte](length1 * 4)
            in.read(tem2)
            var j : Int = 0
            while(j < length1) {
                val num : Int = ByteBuffer.wrap(Arrays.copyOfRange(tem2, j * 4, (j + 1) * 4)).getInt();
                temInvertedIndex.idArray(j) = num
                j += 1
            }
            var tem3 : Array[Byte] = new Array[Byte](length2 * 4)
            in.read(tem3)
            j = 0
            while(j < length2) {
                val num : Int = ByteBuffer.wrap(Arrays.copyOfRange(tem3, j * 4, (j + 1) * 4)).getInt();
                temInvertedIndex.index(j) = num
                j += 1
            }
            myArray(i) = (temMBR, temInvertedIndex)
        }
        return (myMBR, myArray)
    }

    def parseFile(path : String) : (Int, Int, Int) = {
        val fileName : String = path
        var dayID    : Int = 0
        var timeID   : Int = 0
        var spaceID  : Int = 0
        val lines1 : Array[String] = fileName.split("/")
        val lines2 : Array[String] = lines1(lines1.length - 1).split("\\.")
        val line : String = lines2(0);
        if (line.charAt(5) == 's'){
            var firStr = line.slice(4, 5)
            var secStr = line.slice(10, line.length)
            timeID = firStr.toInt
            spaceID = secStr.toInt
        }else{
            var firStr = line.slice(4, 6)
            var secStr = line.slice(11, line.length)
            timeID = firStr.toInt
            spaceID = secStr.toInt
        }
        // 用于billion数据集的join
        val lines3 : String = lines1(lines1.length - 3)
        // 用于trillion数据集的query
        // val lines3 : String = lines1(lines1.length - 2)
        dayID = lines3.slice(3, 4).toInt
        return (dayID, timeID, spaceID)
    }

    def simplificationArrayInTimeWhole(inputArray : Array[Array[Int]]) : Map[Int, ArrayBuffer[Array[Int]]] = {
        var arrayMap : Map[Int, ArrayBuffer[Array[Int]]] = Map[Int, ArrayBuffer[Array[Int]]]()
        for (i <- 0 until inputArray.length) {
            val timeID : Int = inputArray(i)(0)
            if (arrayMap.get(timeID) == None) {
                var tem : ArrayBuffer[Array[Int]] = ArrayBuffer[Array[Int]]()
                tem += inputArray(i)
                arrayMap += timeID -> tem
            } else {
                var tem : ArrayBuffer[Array[Int]] = arrayMap.get(timeID).get
                tem += inputArray(i)
                arrayMap += timeID -> tem
            }
        }
        arrayMap
    }

    // 这块需要更改一下，可能是一个隐藏的bug
    // 可以通过 java File类的length方法获取文件的大小，之后可以将具体的参数输入该函数中，一直读取到文件末尾
    def readByteRandomAccess(readPath : String, beginPos : Int, endPos : Int) : Array[Byte] = {
        var returnData : Array[Byte] = new Array[Byte](endPos - beginPos)
        var fis : FileInputStream = new FileInputStream(new File(readPath))
        fis.skip(beginPos)
        fis.read(returnData)
        fis.close()
        return returnData
    }
}