package src.main.scala.squid

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
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

object SQUIDQuery {
    
    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .config("spark.files.maxPartitionBytes", 25 * 1024 * 1024)
                    .config("spark.driver.maxResultSize", "30g")
                    .appName("SQUID-Query")
                    .getOrCreate()
        val sc = spark.sparkContext

        val myBaseSettings : BaseSetting = new BaseSetting
        myBaseSettings.setTotalSnap(8640)
        myBaseSettings.setBeginSecond(0)
        myBaseSettings.setTimeInterval(5)
        myBaseSettings.setPatIDList(10)
        myBaseSettings.setRecordLength(8)
        myBaseSettings.setLonGridNum(100)
        myBaseSettings.setLatGridNum(100)


        // paramater change 1, default is 150
        myBaseSettings.setDelta(150)
        
        // paramater change 2, default is 240 (20 minutes)
        myBaseSettings.setContiSnap(240)
        
        // paramater change 3, The root path of the data set
        myBaseSettings.setRootPath("file:///mnt/data1/trillion/singapore/")

        // paramater change 4, days num
        myBaseSettings.setDaysNum(1)
        
        // paramater change 5, time partitions num
        myBaseSettings.setTimePartitionsNum(24)
        
        // paramater change 6, space partitions num
        myBaseSettings.setSpacePartitionsNum(45)
        
        // paramater change 7, number of trajectories in a partition
        myBaseSettings.setTrajNumEachSpace(65536)
        
        // paramater change 8, total trajectories num
        myBaseSettings.setTotalTrajNums(2949120)
        
        // paramater change 9, index interval
        myBaseSettings.setIndexSnapInterval(12)

        // paramater change 10, the number of points of a trajectory in a partition
        myBaseSettings.setOneParSnapNum(360)
        
        // paramater change 11, dataset
        myBaseSettings.setDataset("singapore")

        // paramater change 12, whether to write the index to the file，1 is write and 0 is not write
        myBaseSettings.setWriteIndexToFileFlag(0)

        // paramater change 13, set the source of the read index, "zip" or "file"
        myBaseSettings.setReadIndexFromWhere("file")

        var datasetList : Array[String] = Array("singapore", "porto")
        var totalTrajNumList : Array[Int] = Array(10000, 10000, 12500, 15000, 17500, 20000)
        var deltaList : Array[Double] = Array(50, 50, 100, 150, 200, 250)
        var contiSnapList : Array[Int] = Array(120, 120, 180, 240, 300, 360)
        var daysNumList : Array[Int] = Array(0, 1, 2, 3)
        

        // for (currDataset <- datasetList) {
        //     myBaseSettings.setDataset(currDataset)
        //     myBaseSettings.setRootPath("file:///mnt/data1/trillion/" + currDataset + "/")
        //     if (myBaseSettings.dataset == "porto") {
        //         // porto
        //         myBaseSettings.setMINLON((-8.77).toFloat);
        //         myBaseSettings.setMINLAT((40.9).toFloat);
        //         myBaseSettings.setMAXLON((-8.23).toFloat);
        //         myBaseSettings.setMAXLAT((41.5).toFloat);
        //         myBaseSettings.setLonGridLength((0.0054).toFloat);
        //         myBaseSettings.setLatGridLength((0.006).toFloat);
        //     } else {
        //         // singapore
        //         myBaseSettings.setMINLON((103.6).toFloat);
        //         myBaseSettings.setMINLAT((1.22).toFloat);
        //         myBaseSettings.setMAXLON((104.0).toFloat);
        //         myBaseSettings.setMAXLAT((1.58).toFloat);
        //         myBaseSettings.setLonGridLength((0.004).toFloat);
        //         myBaseSettings.setLatGridLength((0.0036).toFloat);
        //     }
        //     val inputFilePath       : Array[String] = MethodCommonFunction.getInitialInputFilePathWhole(myBaseSettings, "day", "/day", "_gzip/par_ori", "zorder", ".tstjs.gz")
        //     val lookupTableFilePath : Array[String] = MethodCommonFunction.getLookupTableFilePathWhole(myBaseSettings, "day", "/day", "_trajic/par_map", ".tstjs")
        //     var lookupTable     : Array[Array[Int]] = MethodCommonFunction.getLookupTableWhole(sc, lookupTableFilePath, myBaseSettings)
        //     val bcLookupTable   : Broadcast[Array[Array[Int]]] = sc.broadcast(lookupTable)
        //     val indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = MethodCommonFunction.setIndexFromFile(sc, myBaseSettings)
            
        //     for (currDelta <- deltaList) {
        //         myBaseSettings.setDelta(currDelta)
        //         doSearchEntry(sc, myBaseSettings, indexRDD, lookupTable)
        //     }
        // }

        // myBaseSettings.setDelta(150)

        // for (currDataset <- datasetList) {
        //     myBaseSettings.setDataset(currDataset)
        //     myBaseSettings.setRootPath("file:///mnt/data1/trillion/" + currDataset + "/")
        //     myBaseSettings.setDataset(currDataset)
        //     myBaseSettings.setRootPath("file:///mnt/data1/trillion/" + currDataset + "/")
        //     if (myBaseSettings.dataset == "porto") {
        //         // porto
        //         myBaseSettings.setMINLON((-8.77).toFloat);
        //         myBaseSettings.setMINLAT((40.9).toFloat);
        //         myBaseSettings.setMAXLON((-8.23).toFloat);
        //         myBaseSettings.setMAXLAT((41.5).toFloat);
        //         myBaseSettings.setLonGridLength((0.0054).toFloat);
        //         myBaseSettings.setLatGridLength((0.006).toFloat);
        //     } else {
        //         // singapore
        //         myBaseSettings.setMINLON((103.6).toFloat);
        //         myBaseSettings.setMINLAT((1.22).toFloat);
        //         myBaseSettings.setMAXLON((104.0).toFloat);
        //         myBaseSettings.setMAXLAT((1.58).toFloat);
        //         myBaseSettings.setLonGridLength((0.004).toFloat);
        //         myBaseSettings.setLatGridLength((0.0036).toFloat);
        //     }
        //     val inputFilePath       : Array[String] = MethodCommonFunction.getInitialInputFilePathWhole(myBaseSettings, "day", "/day", "_gzip/par_ori", "zorder", ".tstjs.gz")
        //     val lookupTableFilePath : Array[String] = MethodCommonFunction.getLookupTableFilePathWhole(myBaseSettings, "day", "/day", "_trajic/par_map", ".tstjs")
        //     var lookupTable     : Array[Array[Int]] = MethodCommonFunction.getLookupTableWhole(sc, lookupTableFilePath, myBaseSettings)
        //     val bcLookupTable   : Broadcast[Array[Array[Int]]] = sc.broadcast(lookupTable)
        //     val indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = MethodCommonFunction.setIndexFromFile(sc, myBaseSettings)
            
        //     for (currContiSnap <- contiSnapList) {
        //         myBaseSettings.setContiSnap(currContiSnap)
        //         doSearchEntry(sc, myBaseSettings, indexRDD, lookupTable)
        //     }
        // }

        // myBaseSettings.setContiSnap(240)
        

        entry(sc, myBaseSettings)
        // entry(sc, myBaseSettings)
        sc.stop()
    }

    def entry(sc : SparkContext, myBaseSettings : BaseSetting) : Unit = {
        if (myBaseSettings.dataset == "porto") {
            // porto
            myBaseSettings.setMINLON((-8.77).toFloat);
            myBaseSettings.setMINLAT((40.9).toFloat);
            myBaseSettings.setMAXLON((-8.23).toFloat);
            myBaseSettings.setMAXLAT((41.5).toFloat);
            myBaseSettings.setLonGridLength((0.0054).toFloat);
            myBaseSettings.setLatGridLength((0.006).toFloat);
        } else {
            // singapore
            myBaseSettings.setMINLON((103.6).toFloat);
            myBaseSettings.setMINLAT((1.22).toFloat);
            myBaseSettings.setMAXLON((104.0).toFloat);
            myBaseSettings.setMAXLAT((1.58).toFloat);
            myBaseSettings.setLonGridLength((0.004).toFloat);
            myBaseSettings.setLatGridLength((0.0036).toFloat);
        }

        val inputFilePath       : Array[String] = MethodCommonFunction.getInitialInputFilePathWhole(myBaseSettings, "day", "/day", "_gzip/par_ori", "zorder", ".tstjs.gz")
        val lookupTableFilePath : Array[String] = MethodCommonFunction.getLookupTableFilePathWhole(myBaseSettings, "day", "/day", "_trajic/par_map", ".tstjs")
        var lookupTable     : Array[Array[Int]] = MethodCommonFunction.getLookupTableWhole(sc, lookupTableFilePath, myBaseSettings)
        val bcLookupTable   : Broadcast[Array[Array[Int]]] = sc.broadcast(lookupTable)
        if (myBaseSettings.readIndexFromWhere == "zip") {
            val indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = MethodCommonFunction.setIndexUsingGzipWhole(sc, inputFilePath, myBaseSettings, bcLookupTable)
            doSearchEntry(sc, myBaseSettings, indexRDD, lookupTable)
        } else {
            val indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = MethodCommonFunction.setIndexFromFile(sc, myBaseSettings)
            doSearchEntry(sc, myBaseSettings, indexRDD, lookupTable)
            doSearchEntry(sc, myBaseSettings, indexRDD, lookupTable)
        }

    }

    def doSearchEntry(sc : SparkContext, myBaseSettings : BaseSetting, indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])], 
                        lookupTable : Array[Array[Int]]) : Unit = {
        val patIDList : Array[Int] = myBaseSettings.patIDList

        var queryResult : Array[TSTJSResult] = new Array[TSTJSResult](patIDList.length)
        var queryResultPos : Int = 0
        patIDList.foreach{patID => {
            val patPathArray : Array[String] = new Array[String](myBaseSettings.daysNum)
            for (i <- 0 until myBaseSettings.daysNum) {
                    // paramater change
                    val temPath : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + "/query/day" + i.toString + "/trajectory"  + patID.toString + ".tstjs"
                    patPathArray(i) = temPath
            }
            val patPath : String = patPathArray.mkString(",")
            val patCoorList : Array[Array[(Float, Float)]] = sc.binaryFiles(patPath)
                                                               .map(l => l._2.toArray)
                                                               .map(mapPatCoor)
                                                               .collect()
            val bcPatCoor : Broadcast[Array[Array[(Float, Float)]]] = sc.broadcast(patCoorList)

            val time1 : Long = System.currentTimeMillis
            var candiList : Array[Array[Int]] = indexRDD.filter(l => filterWhole(l, myBaseSettings, bcPatCoor) == true)
                                                        .map(l => mapSearchWithIndexRangeQueryWhole(l, myBaseSettings, bcPatCoor))
                                                        .collect()
            val time2 : Long = System.currentTimeMillis
            var totalFilterVerifyTime : Double = ((time2 - time1) / 1000.0).toDouble

            var totalFilterTime : Int = 0
            var totalVerifyTime : Int = 0
            var totalReadFileTime : Int = 0
            var totalDecompressTime : Int = 0
            var totalPureVerifyTime : Int = 0
            var totalCandilist : Int = 0
            for (i <- 0 until candiList.length) {
                totalFilterTime += candiList(i)(0)
                totalVerifyTime += candiList(i)(1)
                totalReadFileTime += candiList(i)(2)
                totalDecompressTime += candiList(i)(3)
                totalPureVerifyTime += candiList(i)(4)
                totalCandilist += candiList(i)(5)
                candiList(i) = candiList(i).slice(6, candiList(i).length)
            }
        
            val time7 : Long = System.currentTimeMillis
            var finalResult : Array[Int] = getFinalResultWhole(candiList, myBaseSettings, lookupTable)
            val time8 : Long = System.currentTimeMillis
            var totalMergeTime : Double = ((time8 - time7) / 1000.0).toDouble

            finalResult = finalResult.sortWith(_ < _)
            println("----------------------------------------------------------")
            println("Query time on the index: " + totalFilterVerifyTime)
            println("The length of candiList is: " + candiList.length)
            println("The Time of get the final result is: " + totalMergeTime)
            println("The number of close contacts of the " + patID + "th patient is: " + finalResult.length)
            println("----------------------------------------------------------")
            var temTSTJSResult : TSTJSResult = new TSTJSResult
            temTSTJSResult.setQueryID(patID)
            temTSTJSResult.setTotalQueryTime(totalFilterVerifyTime + totalMergeTime)
            temTSTJSResult.setFilterTime(totalFilterTime)
            temTSTJSResult.setVerifyTime(totalVerifyTime)
            temTSTJSResult.setMergeTime(totalMergeTime)
            temTSTJSResult.setVerifyReadFileTime(totalReadFileTime)
            temTSTJSResult.setVerifyDecompressTime(totalDecompressTime)
            temTSTJSResult.setVerifyPureCalculTime(totalPureVerifyTime)
            temTSTJSResult.setResultLength(finalResult.length)
            temTSTJSResult.setCandilistLength(totalCandilist)
            queryResult(queryResultPos) = temTSTJSResult
            queryResultPos += 1
        }
        }
        val queryResultRootWritePath : String = "/home/sigmod/code/python/sigmod_exper/query_" + myBaseSettings.dataset + "_0.5_" + myBaseSettings.indexSnapInterval.toString + "_tstjs_"
        val queryResultWritePath : String = queryResultRootWritePath + myBaseSettings.totalTrajNums.toString + "_" + myBaseSettings.delta.toInt.toString + "_" + myBaseSettings.contiSnap.toString + ".txt"
        writeQueryResultToFile(queryResultWritePath, queryResult, queryResultPos)
    }

    def mapPatCoor(iter : Array[Byte]) : Array[(Float, Float)] = {
        var resultArray : Array[(Float, Float)] = new Array[(Float, Float)](17280)
        val iterArray   : Array[Array[Byte]] = iter.grouped(14).toArray
        for (i <- 0 until iterArray.length) {
            val l : Array[Byte] = iterArray(i)
            val lon : Float = ByteBuffer.wrap(l.slice(6, 10)).getFloat
            val lat : Float = ByteBuffer.wrap(l.slice(10, 14)).getFloat
            resultArray(i) = (lon, lat)
        }
        resultArray
    }

    def filterWhole(iter : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]), myBaseSettings : BaseSetting, 
                            bcPatCoor : Broadcast[Array[Array[(Float, Float)]]]) : Boolean = {
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        val totalSnap          : Int    = myBaseSettings.totalSnap
        val indexSnapInterval : Int   = myBaseSettings.indexSnapInterval
        val delta : Double = myBaseSettings.delta
        val startSnap   : Int = totalSnap / timePartitionsNum * iter._2
        val stopSnap    : Int = totalSnap / timePartitionsNum * (iter._2 + 1)
        val patCoorList : Array[Array[(Float, Float)]] = bcPatCoor.value
        val lonDiff     : Float = (delta / 111111.0).toFloat
        val latDiff     : Float = (delta / 111111.0).toFloat
        var MINLON : Float = Float.MaxValue
        var MINLAT : Float = Float.MaxValue
        var MAXLON : Float = Float.MinValue
        var MAXLAT : Float = Float.MinValue
        var currSnap : Int = startSnap
        while(currSnap < stopSnap) {
            val lon : Float = patCoorList(iter._1)(currSnap)._1
            val lat : Float = patCoorList(iter._1)(currSnap)._2
            MINLON = math.min(MINLON, lon)
            MAXLON = math.max(MAXLON, lon)
            MINLAT = math.min(MINLAT, lat)
            MAXLAT = math.max(MAXLAT, lat)
            currSnap += indexSnapInterval
        }
        MINLON = MINLON - lonDiff
        MAXLON = MAXLON + lonDiff
        MINLAT = MINLAT - latDiff
        MAXLAT = MAXLAT + latDiff
        val myMBR : MBR = iter._4
        if (myMBR.ifIntersectMBR(new MBR(MINLON, MAXLON, MINLAT, MAXLAT))) {
            return true
        } else {
            return false
        }
    }

    def mapSearchWithIndexRangeQueryWhole(iter : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]), myBaseSettings : BaseSetting, 
                            bcPatCoor : Broadcast[Array[Array[(Float, Float)]]]) : Array[Int] = {
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int    = myBaseSettings.contiSnap
        val totalSnap          : Int    = myBaseSettings.totalSnap
        val delta              : Double = myBaseSettings.delta
        val dataset            : String = myBaseSettings.dataset
        
        val indexSnapInterval : Int   = myBaseSettings.indexSnapInterval
        val oneParSnapNum     : Int   = myBaseSettings.oneParSnapNum
        val lonGridNum        : Int   = myBaseSettings.lonGridNum
        val latGridNum        : Int   = myBaseSettings.latGridNum
        val MINLON            : Float = myBaseSettings.MINLON
        val MINLAT            : Float = myBaseSettings.MINLAT
        var lonGridLength     : Float = 0.0f
        var latGridLength     : Float = 0.0f

        var localhost = InetAddress.getLocalHost()
        var hostname = localhost.getHostName()

        val dayID : Int = iter._1

        val startSnap   : Int = totalSnap / timePartitionsNum * iter._2
        val stopSnap    : Int = totalSnap / timePartitionsNum * (iter._2 + 1)
        val patCoorList : Array[Array[(Float, Float)]] = bcPatCoor.value
        val lonDiff     : Double = delta / 111111.0
        val latDiff     : Double = delta / 111111.0

        val filterBeginTime : Long = System.currentTimeMillis
        var temMap      : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()
        var resultList  : ArrayBuffer[Int] = ArrayBuffer[Int]()
        val myIndex : Array[(MBR, InvertedIndex)] = iter._5
        val stepNum : Int = floor(contiSnap.toDouble / indexSnapInterval).toInt
        var currSnap : Int = startSnap
        var firLength : Int = ceil(((stopSnap - startSnap) - ((stepNum - 1) * indexSnapInterval)).toDouble / indexSnapInterval).toInt
        val resultArray : Array[Array[Int]] = Array.ofDim[Int](80, myBaseSettings.trajNumEachSpace)
        for (i <- 0 until resultArray.length) {
            for (j <- 0 until myBaseSettings.trajNumEachSpace) {
                resultArray(i)(j) = 0
            }
        }
        while(currSnap < stopSnap){
            val pos : Int = (currSnap - startSnap) / indexSnapInterval
            var firPos : Int = 0
            if (pos - (stepNum - 1) >= 0) {
                firPos = pos - (stepNum - 1)
            }
            var temList : ArrayBuffer[Int] = ArrayBuffer[Int]()
            val lon    : Float = patCoorList(dayID)(currSnap)._1
            val lat    : Float = patCoorList(dayID)(currSnap)._2
            val minLon : Float = (lon - lonDiff).toFloat
            val maxLon : Float = (lon + lonDiff).toFloat
            val minLat : Float = (lat - latDiff).toFloat
            val maxLat : Float = (lat + latDiff).toFloat
            val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / 12)
            val myMBR : MBR = curIndex._1
            if (myMBR.ifIntersectMBR(new MBR(minLon, maxLon, minLat, maxLat))) {
                val myInvertedIndex : InvertedIndex = curIndex._2
                lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
                latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum
                var minLonGridID : Int = floor((minLon - myMBR.minLon) / lonGridLength).toInt
                var maxLonGridID : Int = floor((maxLon - myMBR.minLon) / lonGridLength).toInt
                var minLatGridID : Int = floor((minLat - myMBR.minLat) / latGridLength).toInt
                var maxLatGridID : Int = floor((maxLat - myMBR.minLat) / latGridLength).toInt
                if (minLonGridID < 0){
                    minLonGridID = 0
                }
                if (maxLonGridID >= lonGridNum){
                    maxLonGridID = lonGridNum - 1
                }
                if (minLatGridID < 0){
                    minLatGridID = 0
                }
                if (maxLatGridID >= latGridNum){
                    maxLatGridID = latGridNum - 1
                }
        
                for (lonGridID <- minLonGridID to maxLonGridID){
                    for (latGridID <- minLatGridID to maxLatGridID){
                        val gridID   : Int = latGridID * lonGridNum + lonGridID
                        val beginPos : Int = myInvertedIndex.index(gridID * 2)
                        val length   : Int = myInvertedIndex.index(gridID * 2 + 1);
                        for (i <- 0 until length){
                            val keyOrderPos = myInvertedIndex.idArray(beginPos + i)
                            for (j <- firPos to pos) {
                                resultArray(j)(keyOrderPos) += 1
                                if (resultArray(j)(keyOrderPos) == stepNum) {
                                    resultList += keyOrderPos
                                }
                            }
                            temList += keyOrderPos
                        }
                    }
                }
            }
            temMap += currSnap -> temList
            currSnap += indexSnapInterval
        }

        currSnap = startSnap
        resultList ++= temMap.get(currSnap).get

        currSnap = stopSnap - 1
        var temList : ArrayBuffer[Int] = ArrayBuffer[Int]()
        val lon    : Float = patCoorList(dayID)(currSnap)._1
        val lat    : Float = patCoorList(dayID)(currSnap)._2
        val minLon : Float = (lon - lonDiff).toFloat
        val maxLon : Float = (lon + lonDiff).toFloat
        val minLat : Float = (lat - latDiff).toFloat
        val maxLat : Float = (lat + latDiff).toFloat
        val curIndex : (MBR, InvertedIndex) = myIndex(myIndex.length - 1)

        val myMBR : MBR = curIndex._1
        if (myMBR.ifIntersectMBR(new MBR(minLon, maxLon, minLat, maxLat))) {
            val myInvertedIndex : InvertedIndex = curIndex._2
            lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
            latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum
            var minLonGridID : Int = floor((minLon - myMBR.minLon) / lonGridLength).toInt
            var maxLonGridID : Int = floor((maxLon - myMBR.minLon) / lonGridLength).toInt
            var minLatGridID : Int = floor((minLat - myMBR.minLat) / latGridLength).toInt
            var maxLatGridID : Int = floor((maxLat - myMBR.minLat) / latGridLength).toInt
            if (minLonGridID < 0){
                minLonGridID = 0
            }
            if (maxLonGridID >= lonGridNum){
                maxLonGridID = lonGridNum - 1
            }
            if (minLatGridID < 0){
                minLatGridID = 0
            }
            if (maxLatGridID >= latGridNum){
                maxLatGridID = latGridNum - 1
            }
            
            for (lonGridID <- minLonGridID to maxLonGridID){
                for (latGridID <- minLatGridID to maxLatGridID){
                    val gridID   : Int = latGridID * lonGridNum + lonGridID
                    val beginPos : Int = myInvertedIndex.index(gridID * 2)
                    val length   : Int = myInvertedIndex.index(gridID * 2 + 1);
                    for (i <- 0 until length){
                        temList += myInvertedIndex.idArray(beginPos + i)
                    }
                }
            }
        }
        resultList ++= temList
        val filterEndTime : Long = System.currentTimeMillis

        val verifyBeginTime : Long = System.currentTimeMillis

        var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
        var rearResult  : ArrayBuffer[Int] = ArrayBuffer[Int]()
        val timeID : Int = iter._2
        var spaceSet : Set[Int] = Set[Int]()
        var spaceMap : Map[Int, ArrayBuffer[Int]] = Map[Int, ArrayBuffer[Int]]()

        var temBeginTime : Long = 0
        var temEndTime : Long = 0
        var readFileTime : Int = 0
        var decompressTime : Int = 0
        var pureVerifyTime : Int = 0
        var spaceArray : Array[Int] = Array(iter._3)

        var debugLon1 : Float = 0.0.toFloat
        var debugLat1 : Float = 0.0.toFloat
        var debugLon2 : Float = 0.0.toFloat
        var debugLat2 : Float = 0.0.toFloat
        var debugMinLon : Float = 0.0.toFloat
        var debugMaxLon : Float = 0.0.toFloat
        var debugMinLat : Float = 0.0.toFloat
        var debugMaxLat : Float = 0.0.toFloat

        var debugLocateNum : Int = 0
        var debugPointsNum : Int = Int.MaxValue
        var debugInforLength : Int = 0
        for (j <- 0 until spaceArray.length) {
            val spaceID : Int = spaceArray(j)
            // paramater change
            // val readPath : String = myBaseSettings.rootPath + "/" + dataset + "/" + myBaseSettings.totalTrajNums.toString + "/day" + dayID.toString + "Zorder-60min/day" + dayID.toString + "-trajic-60-4000/par_ori" + timeID.toString + "zorder" + spaceID.toString + ".tstjs"
            val readPath : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + "/day" + dayID.toString + "_trajic/par_ori" + timeID.toString + "zorder" + spaceID.toString + ".tstjs"
            
            val trajNumData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0, 4)
            val thisTrajNum : Int = ByteBuffer.wrap(trajNumData).getInt()
            
            val controlInfoLength : Int = thisTrajNum * 4
            temBeginTime = System.currentTimeMillis
            val testData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0 + 4, controlInfoLength + 4)
            temEndTime = System.currentTimeMillis
            readFileTime += (temEndTime - temBeginTime).toInt

            var temArrayBuffer : Array[Int] = resultList.toSet.toArray
            for (k <- 0 until temArrayBuffer.length) {
                val myOrderPos : Int = temArrayBuffer(k)
                var helpArray : Array[Byte] = new Array[Byte](4)
                for (t <- 0 until 4) {
                    helpArray(t) = testData(myOrderPos * 4 + t)
                }
                var startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                var stopPos = 0
                if (myOrderPos < (thisTrajNum - 1)) {
                    for (t <- 0 until 4) {
                        helpArray(t) = testData((myOrderPos + 1) * 4 + t)
                    }
                    stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength

                    temBeginTime = System.currentTimeMillis
                    var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPath, startPos + 4, stopPos + 4)
                    temEndTime = System.currentTimeMillis
                    readFileTime += (temEndTime - temBeginTime).toInt

                    temBeginTime = System.currentTimeMillis
                    var dataPoints : Array[GPSPoint] = CompressMethod.unTrajicGetPoints(helpArray2)
                    temEndTime = System.currentTimeMillis
                    decompressTime += (temEndTime - temBeginTime).toInt

                    temBeginTime = System.currentTimeMillis
                    val beginPos : Int = 0
                    var nowPos : Int = beginPos
                    var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
                    var flag : Int = 0
                    var temConti : Int = 0
                    var maxConti : Int = 0
                    var pos : Int = 0
                    while(pos < oneParSnapNum){
                        val curPoint : GPSPoint = dataPoints(nowPos)
                        val testLon : Float = curPoint.lon
                        val testLat : Float = curPoint.lat
                        nowPos += 1
                        val lon : Float = patCoorList(dayID)(startSnap + pos)._1
                        val lat : Float = patCoorList(dayID)(startSnap + pos)._2
                        debugLon1 = testLon
                        debugLat1 = testLat
                        debugLon2 = lon
                        debugLat2 = lat

                        val minLon : Float = lon - lonDiff.toFloat
                        val maxLon : Float = lon + lonDiff.toFloat
                        val minLat : Float = lat - latDiff.toFloat
                        val maxLat : Float = lat + latDiff.toFloat

                        debugMinLon = minLon
                        debugMaxLon = maxLon
                        debugMinLat = minLat
                        debugMaxLat = maxLat

                        var isLocate : Boolean = false
                        if ((Math.abs(testLon - (-1.0)) < 0.000001) || (Math.abs(testLat - (-1.0)) < 0.000001) ||
                                        (Math.abs(lon - (-1.0)) < 0.000001) || (Math.abs(lat - (-1.0)) < 0.000001)){
                            isLocate = false
                        }else{
                            isLocate = PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat,
                                    maxLat, testLon, testLat)
                            debugLocateNum += 1
                        }
                        if (isLocate){
                            flagArray(pos) = 1
                            if (flag == 0){
                                temConti = 1
                                flag = 1
                            }else{
                                temConti += 1
                            }
                        }else{
                            flagArray(pos) = 0
                            if (flag != 0){
                                flag = 0
                                if (maxConti < temConti){
                                    maxConti = temConti
                                }
                            }
                        }
                        pos += 1
                    }
                    if (maxConti < temConti){
                        maxConti = temConti
                    }
                    if (maxConti >= contiSnap){
                        finalResult += myOrderPos
                    }else{
                        if (flagArray(0) == 1){
                            var count : Int = 0
                            var loop = new Breaks
                            loop.breakable{
                            for (i <- 0 until oneParSnapNum){
                                if (flagArray(i) == 1){
                                    count += 1
                                }else{
                                    loop.break()
                                }
                            }
                            }
                            frontResult += myOrderPos
                            frontResult += count
                        }
                        if (flagArray(oneParSnapNum - 1) == 1){
                            var count : Int = 0
                            var loop = new Breaks
                            loop.breakable{
                            for (i <- oneParSnapNum - 1 to 0 by -1){
                                if (flagArray(i) == 1){
                                count += 1
                            }else{
                                    loop.break()
                                }   
                            }
                            }
                            rearResult += myOrderPos
                            rearResult += count
                        }
                    }
                    temEndTime = System.currentTimeMillis
                    pureVerifyTime += (temEndTime - temBeginTime).toInt
                }
            }
        }
        
        val verifyEndTime : Long = System.currentTimeMillis
        val filterTime : Int = (filterEndTime - filterBeginTime).toInt
        val verifyTime : Int = (verifyEndTime - verifyBeginTime).toInt

        var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
        result += filterTime
        result += verifyTime
        result += readFileTime
        result += decompressTime
        result += pureVerifyTime
        result += resultList.toSet.toArray.length

        result += timeID + dayID * myBaseSettings.timePartitionsNum
        result += spaceArray(0)
        result += finalResult.length
        for (i <- 0 until finalResult.length){
            result += finalResult(i)
        }
        result += frontResult.length
        for (i <- 0 until frontResult.length){
            result += frontResult(i)
        }
        result += rearResult.length
        for (i <- 0 until rearResult.length){
            result += rearResult(i)
        }
        result.toArray
    }

    def getFinalResultWhole(inputArray : Array[Array[Int]], myBaseSettings : BaseSetting, lookupTable : Array[Array[Int]]) : Array[Int] = {
        var finalResult : Set[Int] = Set[Int]()
        var temMap : Map[Int, Int] = Map[Int, Int]()
        var continuousFlag : Boolean = false
        var arrayMap : Map[Int, ArrayBuffer[Array[Int]]] = MethodCommonFunction.simplificationArrayInTimeWhole(inputArray)
        for (i <- 0 until myBaseSettings.timePartitionsNum * myBaseSettings.daysNum){
            if (arrayMap.get(i) == None) {
                continuousFlag = false
            } else {
                var temArrayBuffer : Array[Array[Int]] = arrayMap.get(i).get.toArray
                if (i == 0 || continuousFlag == false) {
                    temMap.clear
                    for (j <- 0 until temArrayBuffer.length) {
                        var temArray : Array[Int] = temArrayBuffer(j)
                        val dayID : Int = temArray(0) / myBaseSettings.timePartitionsNum
                        val timeID : Int = temArray(0) % myBaseSettings.timePartitionsNum
                        val spaceID : Int = temArray(1)
                        temArray = temArray.slice(2, temArray.length)
                        if (temArray.length != 0) {
                            var pos : Int = 0
                            var length : Int = temArray(0)
                            var myArray : Array[Int] = temArray.slice(pos + 1, pos + length + 1)
                            for (k <- 0 until myArray.length){
                                val trajPos : Int = myArray(k)
                                val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                                if (!finalResult.contains(canTrajID)){
                                    finalResult.add(canTrajID)
                                }
                            }
                            pos = pos + length + 1
                            length = temArray(pos)
                            pos = pos + length + 1
                            length = temArray(pos)
                            myArray = temArray.slice(pos + 1, pos + length + 1)
                            for (j <- 0 until myArray.length / 2){
                                val trajPos : Int = myArray(j * 2)
                                val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                                val snapNum : Int = myArray(j * 2 + 1)
                                temMap += canTrajID -> snapNum
                            }
                        }
                    }
                    continuousFlag = true
                } else {
                    for (j <- 0 until temArrayBuffer.length) {
                        var temArray : Array[Int] = temArrayBuffer(j).toArray
                        val dayID : Int = temArray(0) / myBaseSettings.timePartitionsNum
                        val timeID : Int = temArray(0) % myBaseSettings.timePartitionsNum
                        val spaceID : Int = temArray(1)
                        temArray = temArray.slice(2, temArray.length)
                        if (temArray.length != 0) {
                            var pos : Int = 0
                            var length : Int = temArray(0)
                            var myArray : Array[Int] = temArray.slice(pos + 1, pos + length + 1)
                            for (k <- 0 until myArray.length){
                                val trajPos : Int = myArray(k)
                                val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                                if (!finalResult.contains(canTrajID)){
                                    finalResult.add(canTrajID)
                                }
                            }
                            pos = pos + length + 1
                            length = temArray(pos)
                            myArray = temArray.slice(pos + 1, pos + length + 1)
                            for (k <- 0 until myArray.length / 2){
                                val trajPos : Int = myArray(k * 2)
                                val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                                val snapNum : Int = myArray(k * 2 + 1)
                                if (!finalResult.contains(canTrajID)){
                                    if (temMap.contains(canTrajID)){
                                        val lastSnapNum : Int = temMap.get(canTrajID).get
                                        if (lastSnapNum + snapNum >= myBaseSettings.contiSnap){
                                            finalResult.add(canTrajID)
                                        }
                                    }
                                }
                            }
                        }
                    }
                    temMap.clear()
                    for (j <- 0 until temArrayBuffer.length) {
                        var temArray : Array[Int] = temArrayBuffer(j).toArray
                        val dayID : Int = temArray(0) / myBaseSettings.timePartitionsNum
                        val timeID : Int = temArray(0) % myBaseSettings.timePartitionsNum
                        val spaceID : Int = temArray(1)
                        temArray = temArray.slice(2, temArray.length)
                        if (temArray.length != 0) {
                            var pos : Int = 0
                            var length : Int = temArray(0)
                            var myArray : Array[Int] = temArray.slice(pos + 1, pos + length + 1)
                            pos = pos + length + 1
                            length = temArray(pos)
                            myArray = temArray.slice(pos + 1, pos + length + 1)
                            pos = pos + length + 1
                            length = temArray(pos)
                            myArray = temArray.slice(pos + 1, pos + length + 1)
                            for (k <- 0 until myArray.length / 2){
                                val trajPos : Int = myArray(k * 2)
                                val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                                val snapNum : Int = myArray(k * 2 + 1)
                                temMap += canTrajID -> snapNum
                            }
                        }
                    }
                }
            }
        }
        finalResult.toArray
    }

    def writeQueryResultToFile(path : String, queryResult : Array[TSTJSResult], length : Int) : Unit = {
        var writer : BufferedWriter = new BufferedWriter(new FileWriter(new File(path)))
        for (i <- 0 until length) {
            val temTSTJSResult : TSTJSResult = queryResult(i)
            writer.write(temTSTJSResult.queryID.toString + "\t")
            writer.write(temTSTJSResult.totalQueryTime.toString + "\t")
            writer.write(temTSTJSResult.filterTime.toString + "\t")
            writer.write(temTSTJSResult.verifyTime.toString + "\t")
            writer.write(temTSTJSResult.mergeTime.toString + "\t")
            writer.write(temTSTJSResult.verifyReadFileTime.toString + "\t")
            writer.write(temTSTJSResult.verifyDecompressTime.toString + "\t")
            writer.write(temTSTJSResult.verifyPureCalculTime.toString + "\t")
            writer.write(temTSTJSResult.resultLength.toString + "\t")
            writer.write(temTSTJSResult.candilistLength.toString + "\t")
            writer.write("\n")
        }
        writer.close()
    }

}


class TSTJSResult extends Serializable {

    var queryID              : Int = _
    var totalQueryTime       : Double = _
    var filterTime           : Double = _
    var verifyTime           : Double = _
    var mergeTime            : Double = _
    var verifyReadFileTime   : Double = _
    var verifyDecompressTime : Double = _
    var verifyPureCalculTime : Double = _
    var resultLength         : Int    = _
    var candilistLength      : Int    = _

    def setQueryID(queryID : Int) {
        this.queryID = queryID
    }

    def setTotalQueryTime(totalQueryTime : Double) {
        this.totalQueryTime = totalQueryTime
    }

    def setFilterTime(filterTime : Double) {
        this.filterTime = filterTime
    }

    def setVerifyTime(verifyTime : Double) {
        this.verifyTime = verifyTime
    }

    def setMergeTime(mergeTime : Double) {
        this.mergeTime = mergeTime
    }

    def setVerifyReadFileTime(verifyReadFileTime : Double) {
        this.verifyReadFileTime = verifyReadFileTime
    }

    def setVerifyDecompressTime(verifyDecompressTime : Double) {
        this.verifyDecompressTime = verifyDecompressTime
    }

    def setVerifyPureCalculTime(verifyPureCalculTime : Double) {
        this.verifyPureCalculTime = verifyPureCalculTime
    }

    def setResultLength(resultLength : Int) {
        this.resultLength = resultLength
    }

    def setCandilistLength(candilistLength : Int) {
        this.candilistLength = candilistLength
    }
    
}