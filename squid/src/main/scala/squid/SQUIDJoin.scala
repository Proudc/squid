package src.main.scala.squid

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Partitioner

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

import src.main.scala.dataFormat.BaseSetting
import src.main.scala.dataFormat.MBR
import src.main.scala.index.InvertedIndex
import src.main.scala.util.PublicFunc
import src.main.scala.util.CompressMethod
import src.main.scala.selfPartitioner.keyOfPartitioner
import src.main.scala.util.trajic.GPSPoint

object SQUIDJoin {
    
    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .config("spark.files.maxPartitionBytes", 25 * 1024 * 1024)
                    .config("spark.driver.maxResultSize", "8g")
                    .appName("SQUID-Join")
                    .getOrCreate()
        val sc = spark.sparkContext

        
        val myBaseSettings : BaseSetting = new BaseSetting
        myBaseSettings.setTotalSnap(17280)
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
        myBaseSettings.setRootPath("file:///mnt/data1/billion/porto/15000/")
        // myBaseSettings.setRootPath("file:///mnt/data1/trillion/singapore")

        // paramater change 4, days num
        myBaseSettings.setDaysNum(1)
        
        // paramater change 5, time partitions num
        myBaseSettings.setTimePartitionsNum(24)
        
        // paramater change 6, space partitions num
        myBaseSettings.setSpacePartitionsNum(4)
        
        // paramater change 7, number of trajectories in a partition
        myBaseSettings.setTrajNumEachSpace(4000)
        
        // paramater change 8, total trajectories num
        myBaseSettings.setTotalTrajNums(15000)
        
        // paramater change 9, index interval
        myBaseSettings.setIndexSnapInterval(12)

        // paramater change 10, the number of points of a trajectory in a partition
        myBaseSettings.setOneParSnapNum(720)
        
        // paramater change 11, dataset
        myBaseSettings.setDataset("porto")

        // paramater change 12, whether to write the index to the file，1 is write and 0 is not write
        myBaseSettings.setWriteIndexToFileFlag(1)

        // paramater change 13, set the source of the read index, "zip" or "file"
        myBaseSettings.setReadIndexFromWhere("file")

        
        var datasetList : Array[String] = Array("singapore")
        var totalTrajNumList : Array[Int] = Array(10000, 10000, 12500, 15000, 17500, 20000)
        var deltaList : Array[Double] = Array(50, 50, 100, 150, 200, 250)
        var contiSnapList : Array[Int] = Array(120, 120, 180, 240, 300, 360)
        var daysNumList : Array[Int] = Array(0, 1, 2, 3)

        var timePartitionsNumList : Array[Int] = Array(48, 24, 16)
        var oneParSnapNumList : Array[Int] = Array(360, 720, 1080)
        var trajNumEachSpaceList : Array[Int] = Array(1000, 2000, 3000, 4000, 5000, 6000, 8000, 10000, 12000, 15000)
        var spacePartitionsNumList : Array[Int] = Array(15, 8, 5, 4, 3, 3, 2, 2, 2, 1)

        for (i <- 0 until 3){
            for (j <- 0 until 10){
                myBaseSettings.setTimePartitionsNum(timePartitionsNumList(i))
                myBaseSettings.setOneParSnapNum(oneParSnapNumList(i))
                myBaseSettings.setSpacePartitionsNum(spacePartitionsNumList(j))
                myBaseSettings.setTrajNumEachSpace(trajNumEachSpaceList(j))
                entry(sc, myBaseSettings)
            }
        }

        // for (currDataset <- datasetList) {
        //     myBaseSettings.setDataset(currDataset)
        //     for (currTotalTrajNum <- totalTrajNumList) {
        //         myBaseSettings.setRootPath("file:///mnt/data1/billion/" + currDataset + "/" + currTotalTrajNum.toString + "/")
        //         myBaseSettings.setTotalTrajNums(currTotalTrajNum)
        //         var spaceNum : Int = (currTotalTrajNum.toFloat / 4000).ceil.toInt
        //         myBaseSettings.setSpacePartitionsNum(spaceNum)
        //         entry(sc, myBaseSettings)
        //     }
        // }

        // myBaseSettings.setTotalTrajNums(15000)
        // myBaseSettings.setSpacePartitionsNum(4)


        // for (currDataset <- datasetList) {
        //     myBaseSettings.setDataset(currDataset)
        //     myBaseSettings.setRootPath("file:///mnt/data1/billion/" + currDataset + "/15000/")
        //     for (currDelta <- deltaList) {
        //         myBaseSettings.setDelta(currDelta)
        //         entry(sc, myBaseSettings)
        //     }
        // }

        // myBaseSettings.setDelta(150)
        
        // for (currDataset <- datasetList) {
        //     myBaseSettings.setDataset(currDataset)
        //     myBaseSettings.setRootPath("file:///mnt/data1/billion/" + currDataset + "/15000/")
        //     for (currContiSnap <- contiSnapList) {
        //         myBaseSettings.setContiSnap(currContiSnap)
        //         entry(sc, myBaseSettings)
        //     }
        // }

        // myBaseSettings.setContiSnap(240)

        // for (currDataset <- datasetList) {
        //     myBaseSettings.setDataset(currDataset)
        //     myBaseSettings.setRootPath("file:///mnt/data1/billion/" + currDataset + "/15000/")
        //     for (currDaysnum <- daysNumList) {
        //         myBaseSettings.setDaysNum(currDaysnum)
        //         entry(sc, myBaseSettings)
        //     }
        // }

        // entry(sc, myBaseSettings)
        // entry(sc, myBaseSettings)
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

        val inputFilePath       : Array[String] = MethodCommonFunction.getInitialInputFilePathWhole(myBaseSettings, myBaseSettings.trajNumEachSpace.toString + "day", "/day", "_gzip_" + (1440 / myBaseSettings.timePartitionsNum).toString + "/par_ori", "zorder", ".tstjs.gz")
        val lookupTableFilePath : Array[String] = MethodCommonFunction.getLookupTableFilePathWhole(myBaseSettings, myBaseSettings.trajNumEachSpace.toString + "day", "/day", "_zorder_" + (1440 / myBaseSettings.timePartitionsNum).toString + "/par_map", ".tstjs")
        var lookupTable     : Array[Array[Int]] = MethodCommonFunction.getLookupTableWhole(sc, lookupTableFilePath, myBaseSettings)
        val bcLookupTable   : Broadcast[Array[Array[Int]]] = sc.broadcast(lookupTable)
        if (myBaseSettings.readIndexFromWhere == "zip") {
            println("begin to set index")
            val indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = MethodCommonFunction.setIndexUsingGzipWhole(sc, inputFilePath, myBaseSettings, bcLookupTable)
            // doSearchEntryJoin(sc, myBaseSettings, indexRDD, bcLookupTable)
        } else {
            val indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])] = MethodCommonFunction.setIndexFromFile(sc, myBaseSettings)
            doSearchEntryJoin(sc, myBaseSettings, indexRDD, bcLookupTable)
        }





    }

    def doSearchEntryJoin(sc : SparkContext, myBaseSettings : BaseSetting, indexRDD : RDD[(Int, Int, Int, MBR, Array[(MBR, InvertedIndex)])], 
                        bcLookupTable : Broadcast[Array[Array[Int]]]) : Unit = {

        val time1 : Long = System.currentTimeMillis
        var candiList : Array[Array[Int]] = indexRDD.map(l => mapSearchWithIndexRangeQueryWholeJoin(l, myBaseSettings, bcLookupTable))
                                                                        .flatMap(l => (l))
                                                                        .partitionBy(new keyOfPartitioner(myBaseSettings.totalTrajNums))
                                                                        .map(l => l._2.toArray)
                                                                        .glom()
                                                                        .map(l => getFinalResultWholeJoin(l, myBaseSettings, bcLookupTable))
                                                                        .collect()
        val time2 : Long = System.currentTimeMillis
        var totalJoinTime : Double = ((time2 - time1) / 1000.0).toDouble

        var finalR : Set[(Int, Int)] = Set[(Int, Int)]()
        for (i <- 0 until candiList.length) {
            for (j <- 0 until candiList(i).length) {
                if (i < candiList(i)(j)) {
                    finalR += ((i, candiList(i)(j)))
                } else {
                    finalR += ((candiList(i)(j), i))
                }
            }
        }
        
        println("----------------------------------------------------------")
        println("Query time on the index: " + totalJoinTime)
        println("Total pair is: " + finalR.size)
        println("----------------------------------------------------------")
        val queryResultRootWritePath : String = "/home/sigmod/code/python/sigmod_exper/squid_join_" + myBaseSettings.timePartitionsNum + "_" + myBaseSettings.spacePartitionsNum + "_" + myBaseSettings.dataset + "_" + myBaseSettings.daysNum.toString + "_"
        val queryResultWritePath : String = queryResultRootWritePath + myBaseSettings.totalTrajNums.toString + "_" + myBaseSettings.delta.toInt.toString + "_" + myBaseSettings.contiSnap.toString + ".txt"
        var writer : BufferedWriter = new BufferedWriter(new FileWriter(new File(queryResultWritePath)))
        writer.write(totalJoinTime.toString + "\t" + finalR.size.toString)
        writer.close()
    }

    def mapSearchWithIndexRangeQueryWholeJoin(iter : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]), myBaseSettings : BaseSetting,
                                        bcLookupTable : Broadcast[Array[Array[Int]]]) : Array[(Int, ArrayBuffer[Int])] = {
        var totalPairCount = 0
        var totalSecCount = 0
        var totalInterPar = 0
        var total2 = 0
        var flag132269 = 0
        var cacheCount = 0
        var lookupTable = bcLookupTable.value
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int    = myBaseSettings.contiSnap
        val totalSnap          : Int    = myBaseSettings.totalSnap
        val delta              : Double = myBaseSettings.delta
        var trajNumEachSpace   : Int    = myBaseSettings.trajNumEachSpace
        var dataset            : String = myBaseSettings.dataset
        
        
        val indexSnapInterval : Int   = myBaseSettings.indexSnapInterval
        val oneParSnapNum     : Int   = myBaseSettings.oneParSnapNum
        val lonGridNum        : Int   = myBaseSettings.lonGridNum
        val latGridNum        : Int   = myBaseSettings.latGridNum
        val MINLON            : Float = myBaseSettings.MINLON
        val MINLAT            : Float = myBaseSettings.MINLAT
        var lonGridLength     : Float = 0.0f
        var latGridLength     : Float = 0.0f

        val dayID  : Int = iter._1
        val timeID : Int = iter._2

        val startSnap : Int = totalSnap / timePartitionsNum * iter._2
        val stopSnap  : Int = totalSnap / timePartitionsNum * (iter._2 + 1)
        val lonDiff   : Double = delta / 111111.0
        val latDiff   : Double = delta / 111111.0
        var temMap    : Map[Int, Map[Int, Set[Int]]] = Map[Int, Map[Int, Set[Int]]]()
        val myIndex   : Array[(MBR, InvertedIndex)] = iter._5
        var currSnap  : Int = startSnap
        

        val filterBeginTime : Long = System.currentTimeMillis
        val filterBeginTime1 : Long = System.currentTimeMillis
        while(currSnap < stopSnap) {
            var temList : Map[Int, Set[Int]] = Map[Int, Set[Int]]()
            val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / indexSnapInterval)
            val myMBR : MBR = curIndex._1
            val myInvertedIndex : InvertedIndex = curIndex._2
            lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
            latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum

            for (i <- 0 until lonGridNum) {
                for (j <- 0 until latGridNum) {
                    var gridID : Int = j * lonGridNum + i
                    if (myInvertedIndex.index(gridID * 2 + 1) != 0) {
                        var minLon : Float = (myMBR.minLon + i * lonGridLength - lonDiff).toFloat
                        var maxLon : Float = (myMBR.minLon + (i + 1) * lonGridLength + lonDiff).toFloat
                        var minLat : Float = (myMBR.minLat + j * latGridLength - latDiff).toFloat
                        var maxLat : Float = (myMBR.minLat + (j + 1) * latGridLength + latDiff).toFloat
                        var minLonGridID : Int = math.floor((minLon - myMBR.minLon) / lonGridLength).toInt
                        var maxLonGridID : Int = math.floor((maxLon - myMBR.minLon) / lonGridLength).toInt
                        var minLatGridID : Int = math.floor((minLat - myMBR.minLat) / latGridLength).toInt
                        var maxLatGridID : Int = math.floor((maxLat - myMBR.minLat) / latGridLength).toInt
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
                        val trajIDOrigin : Set[Int] = Set[Int]()
                        val trajIDExpand : Set[Int] = Set[Int]()
                        
                        for (lonGridID <- minLonGridID to maxLonGridID){
                            for (latGridID <- minLatGridID to maxLatGridID){
                                gridID = latGridID * lonGridNum + lonGridID
                                val beginPos : Int = myInvertedIndex.index(gridID * 2)
                                val length   : Int = myInvertedIndex.index(gridID * 2 + 1)

                                if ((lonGridID == i) && (latGridID == j)) {
                                    for (k <- 0 until length){
                                        trajIDOrigin += myInvertedIndex.idArray(beginPos + k)
                                    }
                                } else {
                                    for (k <- 0 until length){
                                        trajIDExpand += myInvertedIndex.idArray(beginPos + k)
                                    }
                                }
                            }
                        }
                        for (firTrajID <- trajIDOrigin) {
                            var x = temList.get(firTrajID)
                            if (x == None) {
                                var mySet : Set[Int] = Set[Int]()
                                mySet ++= trajIDOrigin
                                mySet ++= trajIDExpand
                                temList += firTrajID -> mySet
                            } else {
                                var mySet : Set[Int] = x.get
                                mySet ++= trajIDOrigin
                                mySet ++= trajIDExpand
                                temList += firTrajID -> mySet
                            }
                        }
                        for (firTrajID <- trajIDExpand) {
                            var x = temList.get(firTrajID)
                            if (x == None) {
                                var mySet : Set[Int] = Set[Int]()
                                mySet ++= trajIDOrigin
                                temList += firTrajID -> mySet
                            } else {
                                var mySet : Set[Int] = x.get
                                mySet ++= trajIDOrigin
                                temList += firTrajID -> mySet
                            }
                        }

                    }
                }
            }
            temMap += currSnap -> temList
            currSnap += indexSnapInterval
        }
        val filterEndTime1 : Long = System.currentTimeMillis


        val filterBeginTime2 : Long = System.currentTimeMillis
        var resultList  : Map[Int, Set[Int]] = Map[Int, Set[Int]]()
        var resultFlag : Array[Array[Byte]] = Array.ofDim[Byte](trajNumEachSpace, trajNumEachSpace)
        for (i <- 0 until trajNumEachSpace) {
            for (j <- 0 until trajNumEachSpace) {
                resultFlag(i)(j) = 0
            }
        }
        currSnap = startSnap
        var temList : Map[Int, Set[Int]] = temMap.get(currSnap).get
        for (keyid <- temList.keySet) {
            var firSet : Set[Int] = temList.get(keyid).get
            var x = resultList.get(keyid)
            if (x != None) {
                var mySet : Set[Int] = x.get
                for (i <- firSet) {
                    mySet += i
                    resultFlag(keyid)(i) = 1.toByte
                    resultFlag(i)(keyid) = 1.toByte
                }
                resultList += keyid -> mySet
            } else {
                var mySet : Set[Int] = Set[Int]()
                for (i <- firSet) {
                    mySet += i
                    resultFlag(keyid)(i) = 1.toByte
                    resultFlag(i)(keyid) = 1.toByte
                }
                resultList += keyid -> mySet
            }
        }
        val filterEndTime2 : Long = System.currentTimeMillis

        
        val filterBeginTime4 : Long = System.currentTimeMillis
        currSnap = stopSnap - 1
        temList = Map[Int, Set[Int]]()
        val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / indexSnapInterval)
        val myMBR : MBR = curIndex._1
        val myInvertedIndex : InvertedIndex = curIndex._2
        lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
        latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum
        for (i <- 0 until lonGridNum) {
            for (j <- 0 until latGridNum) {
                var gridID : Int = j * lonGridNum + i
                if (myInvertedIndex.index(gridID * 2 + 1) != 0) {
                    var minLon : Float = (myMBR.minLon + i * lonGridLength - lonDiff).toFloat
                    var maxLon : Float = (myMBR.minLon + (i + 1) * lonGridLength + lonDiff).toFloat
                    var minLat : Float = (myMBR.minLat + j * latGridLength - latDiff).toFloat
                    var maxLat : Float = (myMBR.minLat + (j + 1) * latGridLength + latDiff).toFloat
                    var minLonGridID : Int = math.floor((minLon - myMBR.minLon) / lonGridLength).toInt
                    var maxLonGridID : Int = math.floor((maxLon - myMBR.minLon) / lonGridLength).toInt
                    var minLatGridID : Int = math.floor((minLat - myMBR.minLat) / latGridLength).toInt
                    var maxLatGridID : Int = math.floor((maxLat - myMBR.minLat) / latGridLength).toInt
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
                    val trajIDOrigin : Set[Int] = Set[Int]()
                    val trajIDExpand : Set[Int] = Set[Int]()
                    for (lonGridID <- minLonGridID to maxLonGridID){
                        for (latGridID <- minLatGridID to maxLatGridID){
                            gridID = latGridID * lonGridNum + lonGridID
                            val beginPos : Int = myInvertedIndex.index(gridID * 2)
                            val length   : Int = myInvertedIndex.index(gridID * 2 + 1)
                            if ((lonGridID == i) && (latGridID == j)) {
                                for (k <- 0 until length){
                                    trajIDOrigin += myInvertedIndex.idArray(beginPos + k)
                                }
                            }
                            for (k <- 0 until length){
                                trajIDExpand += myInvertedIndex.idArray(beginPos + k)
                            }

                        }
                    }

                    for (firTrajID <- trajIDOrigin) {
                        var x = temList.get(firTrajID)
                        if (x == None) {
                            var mySet : Set[Int] = Set[Int]()
                            mySet ++= trajIDOrigin
                            mySet ++= trajIDExpand
                            temList += firTrajID -> mySet
                        } else {
                            var mySet : Set[Int] = x.get
                            mySet ++= trajIDOrigin
                            mySet ++= trajIDExpand
                            temList += firTrajID -> mySet
                        }
                    }
                    for (firTrajID <- trajIDExpand) {
                        var x = temList.get(firTrajID)
                        if (x == None) {
                            var mySet : Set[Int] = Set[Int]()
                            mySet ++= trajIDOrigin
                            temList += firTrajID -> mySet
                        } else {
                            var mySet : Set[Int] = x.get
                            mySet ++= trajIDOrigin
                            temList += firTrajID -> mySet
                        }
                    }
                }
                
            
        
            }
        }
        for (keyid <- temList.keySet) {
            var firSet : Set[Int] = temList.get(keyid).get
            var x = resultList.get(keyid)
            if (x != None) {
                var mySet : Set[Int] = x.get
                for (i <- firSet) {
                    
                    if ((resultFlag(keyid)(i) == 0) && (resultFlag(i)(keyid) == 0)) {
                        mySet += i
                        resultFlag(keyid)(i) = 1.toByte
                        resultFlag(i)(keyid) = 1.toByte
                    }
                }
                resultList += keyid -> mySet
            } else {
                var mySet : Set[Int] = Set[Int]()
                for (i <- firSet) {
                    
                    mySet += i
                    resultFlag(keyid)(i) = 1.toByte
                    resultFlag(i)(keyid) = 1.toByte
                }
                resultList += keyid -> mySet
            }
        }
        
        
        val filterEndTime4 : Long = System.currentTimeMillis

        val filterBeginTime3 : Long = System.currentTimeMillis
        var filterTime5 : Int = 0
        var filterTime6 : Int = 0
        var filterTime7 : Int = 0
        var filterTime8 : Int = 0
        val stepNum : Int = math.floor(contiSnap.toDouble / indexSnapInterval).toInt
        var helpMap : Map[(Int, Int), Int] = Map[(Int, Int), Int]()
        var lastSnap : Int = -1
        currSnap = startSnap + indexSnapInterval
        while((currSnap + (stepNum - 1) * indexSnapInterval) < stopSnap) {
            temList = temMap.get(currSnap).get
            if (currSnap == (startSnap + indexSnapInterval)) {
                for (keyid <- temList.keySet) {
                    val filterBeginTime6 : Long = System.currentTimeMillis
                    var firSet : Set[Int] = temList.get(keyid).get
                    for (i <- firSet) {
                        if ((resultFlag(keyid)(i) == 0) && (resultFlag(i)(keyid) == 0)) {
                            helpMap += (keyid, i) -> 1
                        }
                    }
                    val filterEndTime6 : Long = System.currentTimeMillis
                    filterTime6 = filterTime6 + (filterEndTime6 - filterBeginTime6).toInt
                    for (i <- 0 until stepNum) {
                        if (i != 0) {
                            var secMap : Map[Int, Set[Int]] = temMap.get(currSnap + i * indexSnapInterval).get
                            var secList : Set[Int] = Set[Int]()
                            var x = secMap.get(keyid)
                            if (x != None) {
                                secList = x.get
                                val filterBeginTime5 : Long = System.currentTimeMillis
                                for (j <- secList) {
                                    if ((resultFlag(keyid)(j) == 0) && (resultFlag(j)(keyid) == 0)) {
                                        var y = helpMap.get((keyid, j))
                                        if (y != None) {
                                            var count : Int = y.get
                                            helpMap += (keyid, j) -> (count + 1)
                                        } else {
                                            helpMap += (keyid, j) -> 1
                                        }
                                    }
                                    
                                }
                                val filterEndTime5 : Long = System.currentTimeMillis
                                filterTime5 = filterTime5 + (filterEndTime5 - filterBeginTime5).toInt
                            }
                            firSet = firSet intersect secList
                        }
                    }
                    var x = resultList.get(keyid)
                    if (x != None) {
                        var mySet : Set[Int] = x.get
                        mySet ++= firSet
                        resultList += keyid -> mySet
                        for (j <- firSet) {
                            resultFlag(keyid)(j) = 1.toByte
                        }
                    } else {
                        var mySet : Set[Int] = Set[Int]()
                        mySet ++= firSet
                        resultList += keyid -> mySet
                        for (j <- firSet) {
                            resultFlag(keyid)(j) = 1.toByte
                        }
                    }
                }
            } else {
                val filterBeginTime7 : Long = System.currentTimeMillis
                var lastList : Map[Int, Set[Int]] = temMap.get(lastSnap).get
                for (keyid <- lastList.keySet) {
                    var firSet : Set[Int] = lastList.get(keyid).get
                    for (i <- firSet) {
                        if ((resultFlag(keyid)(i) == 0) && (resultFlag(i)(keyid) == 0)) {
                            var x = helpMap.get((keyid, i))
                            if (x != None) {
                                var count : Int = x.get
                                helpMap += (keyid, i) -> (count - 1)
                            }
                        }
                        
                    }
                }
                val filterEndTime7 : Long = System.currentTimeMillis
                filterTime7 = filterTime7 + (filterEndTime7 - filterBeginTime7).toInt


                val filterBeginTime8 : Long = System.currentTimeMillis
                var nextList : Map[Int, Set[Int]] = temMap.get(currSnap + (stepNum - 1) * indexSnapInterval).get
                for (keyid <- nextList.keySet) {
                    var resultSet : Set[Int] = Set[Int]()
                    var firSet : Set[Int] = nextList.get(keyid).get
                    for (i <- firSet) {
                        if ((resultFlag(keyid)(i) == 0) && (resultFlag(i)(keyid) == 0)) {
                            var x = helpMap.get((keyid, i))
                            if (x != None) {
                                var count : Int = x.get
                                helpMap += (keyid, i) -> (count + 1)
                                if ((count + 1) == stepNum) {
                                    resultSet += i
                                }
                            } else {
                                helpMap += (keyid, i) -> 1
                            }
                        }
                        
                    }
                    var x = resultList.get(keyid)
                    if (x != None) {
                        var mySet : Set[Int] = x.get
                        mySet ++= resultSet
                        resultList += keyid -> mySet
                        for (j <- resultSet) {
                            resultFlag(keyid)(j) = 1.toByte
                        }
                    } else {
                        var mySet : Set[Int] = Set[Int]()
                        mySet ++= resultSet
                        resultList += keyid -> mySet
                        for (j <- resultSet) {
                            resultFlag(keyid)(j) = 1.toByte
                        }
                    }
                    

                }
                val filterEndTime8 : Long = System.currentTimeMillis
                filterTime8 = filterTime8 + (filterEndTime8 - filterBeginTime8).toInt
            }
            lastSnap = currSnap
            currSnap += indexSnapInterval
            
        }
        val filterEndTime3 : Long = System.currentTimeMillis


        var myLargeMBRminLon : Float = (iter._4.minLon - lonDiff).toFloat
        var myLargeMBRmaxLon : Float = (iter._4.maxLon + lonDiff).toFloat
        var myLargeMBRminLat : Float = (iter._4.minLat - latDiff).toFloat
        var myLargeMBRmaxLat : Float = (iter._4.maxLat + latDiff).toFloat
        var time2 : Int = 0
        val beginTime2 : Long = System.currentTimeMillis
        var resultListSec  : Map[(Int, Int), Set[Int]] = Map[(Int, Int), Set[Int]]()

        for (i <- 0 until myBaseSettings.spacePartitionsNum) {
            if (i != iter._3) {
                var indexPath : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + myBaseSettings.trajNumEachSpace.toString + "day" + (dayID).toString + "/day" + (dayID).toString + "_index_" + (1440 / myBaseSettings.timePartitionsNum).toString + "/time" + iter._2.toString + "space" + i.toString + ".txt"
                var par1 : (MBR, Array[(MBR, InvertedIndex)]) = readIndexFromFilePath(indexPath)
                var temPartition : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]) = (iter._1, iter._2, i, par1._1, par1._2)
                if (temPartition._4.ifIntersectMBR(new MBR(myLargeMBRminLon, myLargeMBRmaxLon, myLargeMBRminLat, myLargeMBRmaxLat))) {
                    total2 += 1
                    var temMapSec : Map[Int, Map[Int, Set[Int]]] = Map[Int, Map[Int, Set[Int]]]()
                    currSnap = startSnap
                    while(currSnap < stopSnap) {
                        var temList : Map[Int, Set[Int]] = Map[Int, Set[Int]]()
                        val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / indexSnapInterval)
                        val secIndex : (MBR, InvertedIndex) = temPartition._5((currSnap - startSnap) / indexSnapInterval)
                        
                        val myMBR : MBR = curIndex._1
                        val myInvertedIndex : InvertedIndex = curIndex._2
                        val secMBR : MBR = secIndex._1
                        val secInvertedIndex : InvertedIndex = secIndex._2
                        
                        lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
                        latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum
                        var lonGridLengthSec = (secMBR.maxLon - secMBR.minLon) / lonGridNum
                        var latGridLengthSec = (secMBR.maxLat - secMBR.minLat) / latGridNum
                        
                        for (i <- 0 until lonGridNum) {
                            for (j <- 0 until latGridNum) {
                                var gridID : Int = j * lonGridNum + i
                                var minLon : Float = (myMBR.minLon + i * lonGridLength - lonDiff).toFloat
                                var maxLon : Float = (myMBR.minLon + (i + 1) * lonGridLength + lonDiff).toFloat
                                var minLat : Float = (myMBR.minLat + j * latGridLength - latDiff).toFloat
                                var maxLat : Float = (myMBR.minLat + (j + 1) * latGridLength + latDiff).toFloat
                                if (secMBR.ifIntersectMBR(new MBR(minLon, maxLon, minLat, maxLat))) {
                                    var minLonGridID : Int = math.floor((minLon - secMBR.minLon) / lonGridLengthSec).toInt
                                    var maxLonGridID : Int = math.floor((maxLon - secMBR.minLon) / lonGridLengthSec).toInt
                                    var minLatGridID : Int = math.floor((minLat - secMBR.minLat) / latGridLengthSec).toInt
                                    var maxLatGridID : Int = math.floor((maxLat - secMBR.minLat) / latGridLengthSec).toInt
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
                                    val trajIDOrigin : Set[Int] = Set[Int]()
                                    val trajIDExpand : Set[Int] = Set[Int]()

                                    for (lonGridID <- minLonGridID to maxLonGridID){
                                        for (latGridID <- minLatGridID to maxLatGridID){
                                            gridID = latGridID * lonGridNum + lonGridID
                                            val beginPos : Int = secInvertedIndex.index(gridID * 2)
                                            val length   : Int = secInvertedIndex.index(gridID * 2 + 1)
                                            for (k <- 0 until length){
                                                trajIDExpand += secInvertedIndex.idArray(beginPos + k)
                                            }
                                        }
                                    }
                                    gridID = j * lonGridNum + i
                                    val beginPos : Int = myInvertedIndex.index(gridID * 2)
                                    val length   : Int = myInvertedIndex.index(gridID * 2 + 1)
                                    for (k <- 0 until length){
                                        trajIDOrigin += myInvertedIndex.idArray(beginPos + k)
                                    }

                                    for (firTrajID <- trajIDOrigin) {
                                        var x = temList.get(firTrajID)
                                        if (x == None) {
                                            var mySet : Set[Int] = Set[Int]()
                                            mySet ++= trajIDExpand
                                            temList += firTrajID -> mySet
                                        } else {
                                            var mySet : Set[Int] = x.get
                                            mySet ++= trajIDExpand
                                            temList += firTrajID -> mySet
                                        }
                                    }
                                }
                            }
                        }
                        temMapSec += currSnap -> temList
                        currSnap += indexSnapInterval
                    }
                    
                    var resultFlagSec : Array[Array[Byte]] = Array.ofDim[Byte](trajNumEachSpace, trajNumEachSpace)
                    for (i <- 0 until trajNumEachSpace) {
                        for (j <- 0 until trajNumEachSpace) {
                            resultFlagSec(i)(j) = 0
                        }
                    }
                    currSnap = startSnap
                    var temListSec : Map[Int, Set[Int]] = temMapSec.get(currSnap).get
                    for (keyid <- temListSec.keySet) {
                        var firSet : Set[Int] = temListSec.get(keyid).get
                        var x = resultListSec.get((temPartition._3, keyid))
                        if (x != None) {
                            var mySet : Set[Int] = x.get
                            for (i <- firSet) {
                                mySet += i
                                resultFlagSec(keyid)(i) = 1.toByte
                                resultFlagSec(i)(keyid) = 1.toByte

                            }
                            resultListSec += (temPartition._3, keyid) -> mySet
                        } else {
                            var mySet : Set[Int] = Set[Int]()
                            for (i <- firSet) {
                                mySet += i
                                resultFlagSec(keyid)(i) = 1.toByte
                                resultFlagSec(i)(keyid) = 1.toByte
                            }
                            resultListSec += (temPartition._3, keyid) -> mySet
                        }
                    }

                    currSnap = stopSnap - 1
                    temListSec = Map[Int, Set[Int]]()
                    val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / indexSnapInterval)
                    val secIndex : (MBR, InvertedIndex) = temPartition._5((currSnap - startSnap) / indexSnapInterval)
                    val myMBR : MBR = curIndex._1
                    val myInvertedIndex : InvertedIndex = curIndex._2
                    val secMBR : MBR = secIndex._1
                    val secInvertedIndex : InvertedIndex = secIndex._2
                    lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
                    latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum
                    var lonGridLengthSec = (secMBR.maxLon - secMBR.minLon) / lonGridNum
                    var latGridLengthSec = (secMBR.maxLat - secMBR.minLat) / latGridNum
                    for (i <- 0 until lonGridNum) {
                        for (j <- 0 until latGridNum) {
                            var minLon : Float = (myMBR.minLon + i * lonGridLength - lonDiff).toFloat
                            var maxLon : Float = (myMBR.minLon + (i + 1) * lonGridLength + lonDiff).toFloat
                            var minLat : Float = (myMBR.minLat + j * latGridLength - latDiff).toFloat
                            var maxLat : Float = (myMBR.minLat + (j + 1) * latGridLength + latDiff).toFloat
                            if (secMBR.ifIntersectMBR(new MBR(minLon, maxLon, minLat, maxLat))) {
                                var minLonGridID : Int = math.floor((minLon - secMBR.minLon) / lonGridLengthSec).toInt
                                var maxLonGridID : Int = math.floor((maxLon - secMBR.minLon) / lonGridLengthSec).toInt
                                var minLatGridID : Int = math.floor((minLat - secMBR.minLat) / latGridLengthSec).toInt
                                var maxLatGridID : Int = math.floor((maxLat - secMBR.minLat) / latGridLengthSec).toInt
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
                                val trajIDOrigin : Set[Int] = Set[Int]()
                                val trajIDExpand : Set[Int] = Set[Int]()
                                for (lonGridID <- minLonGridID to maxLonGridID){
                                    for (latGridID <- minLatGridID to maxLatGridID){
                                        var gridID : Int = latGridID * lonGridNum + lonGridID
                                        val beginPos : Int = secInvertedIndex.index(gridID * 2)
                                        val length   : Int = secInvertedIndex.index(gridID * 2 + 1)
                                        for (k <- 0 until length){
                                            trajIDExpand += secInvertedIndex.idArray(beginPos + k)
                                        }
                                    }
                                }
                                val gridID : Int = j * lonGridNum + i
                                val beginPos : Int = myInvertedIndex.index(gridID * 2)
                                val length   : Int = myInvertedIndex.index(gridID * 2 + 1)
                                for (k <- 0 until length){
                                    trajIDOrigin += myInvertedIndex.idArray(beginPos + k)
                                }

                                for (firTrajID <- trajIDOrigin) {
                                    var x = temListSec.get(firTrajID)
                                    if (x == None) {
                                        var mySet : Set[Int] = Set[Int]()
                                        mySet ++= trajIDExpand
                                        temListSec += firTrajID -> mySet
                                    } else {
                                        var mySet : Set[Int] = x.get
                                        mySet ++= trajIDExpand
                                        temListSec += firTrajID -> mySet
                                    }
                                }
                            }
                        }
                    }
                    for (keyid <- temListSec.keySet) {
                        var firSet : Set[Int] = temListSec.get(keyid).get
                        var x = resultListSec.get((temPartition._3, keyid))
                        if (x != None) {
                            var mySet : Set[Int] = x.get
                            for (i <- firSet) {
                                var testFirID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + keyid * 3 + 2)
                                var testSecID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + temPartition._3 * myBaseSettings.trajNumEachSpace * 3 + i * 3 + 2)

                                if (testFirID == 13 && testSecID == 2269 && timeID == 10) {
                                    flag132269 += 1
                                }
                                if ((resultFlagSec(keyid)(i) == 0) && (resultFlagSec(i)(keyid) == 0)) {
                                    mySet += i
                                    resultFlagSec(keyid)(i) = 1.toByte
                                    resultFlagSec(i)(keyid) = 1.toByte
                                }
                            }
                            resultListSec += (temPartition._3, keyid) -> mySet
                        } else {
                            var mySet : Set[Int] = Set[Int]()
                            for (i <- firSet) {
                                var testFirID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + keyid * 3 + 2)
                                var testSecID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + temPartition._3 * myBaseSettings.trajNumEachSpace * 3 + i * 3 + 2)

                                if (testFirID == 13 && testSecID == 2269 && timeID == 10) {
                                    flag132269 += 1
                                }
                                mySet += i
                                resultFlagSec(keyid)(i) = 1.toByte
                                resultFlagSec(i)(keyid) = 1.toByte
                            }
                            resultListSec += (temPartition._3, keyid) -> mySet
                        }
                    }
                    var helpMap : Map[(Int, Int), Int] = Map[(Int, Int), Int]()
                    var lastSnap : Int = -1
                    currSnap = startSnap + indexSnapInterval
                    while((currSnap + (stepNum - 1) * indexSnapInterval) < stopSnap) {
                        temListSec = temMapSec.get(currSnap).get
                        if (currSnap == (startSnap + indexSnapInterval)) {
                            for (keyid <- temListSec.keySet) {
                                val filterBeginTime6 : Long = System.currentTimeMillis
                                var firSet : Set[Int] = temListSec.get(keyid).get
                                for (i <- firSet) {
                                    if ((resultFlagSec(keyid)(i) == 0) && (resultFlagSec(i)(keyid) == 0)) {
                                        helpMap += (keyid, i) -> 1
                                    }
                                }
                                val filterEndTime6 : Long = System.currentTimeMillis
                                filterTime6 = filterTime6 + (filterEndTime6 - filterBeginTime6).toInt
                                for (i <- 0 until stepNum) {
                                    if (i != 0) {
                                        var secMap : Map[Int, Set[Int]] = temMapSec.get(currSnap + i * indexSnapInterval).get
                                        var secList : Set[Int] = Set[Int]()
                                        var x = secMap.get(keyid)
                                        if (x != None) {
                                            secList = x.get
                                            val filterBeginTime5 : Long = System.currentTimeMillis
                                            for (j <- secList) {
                                                if ((resultFlagSec(keyid)(j) == 0) && (resultFlagSec(j)(keyid) == 0)) {
                                                    var y = helpMap.get((keyid, j))
                                                    if (y != None) {
                                                        var count : Int = y.get
                                                        helpMap += (keyid, j) -> (count + 1)
                                                    } else {
                                                        helpMap += (keyid, j) -> 1
                                                    }
                                                }

                                            }
                                            val filterEndTime5 : Long = System.currentTimeMillis
                                            filterTime5 = filterTime5 + (filterEndTime5 - filterBeginTime5).toInt
                                        }
                                        firSet = firSet intersect secList
                                    }
                                }
                                var x = resultListSec.get((temPartition._3, keyid))
                                if (x != None) {
                                    var mySet : Set[Int] = x.get
                                    mySet ++= firSet
                                    resultListSec += (temPartition._3, keyid) -> mySet
                                    for (j <- firSet) {
                                        resultFlagSec(keyid)(j) = 1.toByte
                                    }
                                } else {
                                    var mySet : Set[Int] = Set[Int]()
                                    mySet ++= firSet
                                    resultListSec += (temPartition._3, keyid) -> mySet
                                    for (j <- firSet) {
                                        resultFlagSec(keyid)(j) = 1.toByte
                                    }
                                }
                            }
                        } else {
                            val filterBeginTime7 : Long = System.currentTimeMillis
                            var lastList : Map[Int, Set[Int]] = temMapSec.get(lastSnap).get
                            for (keyid <- lastList.keySet) {
                                var firSet : Set[Int] = lastList.get(keyid).get
                                for (i <- firSet) {
                                    if ((resultFlagSec(keyid)(i) == 0) && (resultFlagSec(i)(keyid) == 0)) {
                                        var x = helpMap.get((keyid, i))
                                        if (x != None) {
                                            var count : Int = x.get
                                            helpMap += (keyid, i) -> (count - 1)
                                        }
                                    }

                                }
                            }
                            val filterEndTime7 : Long = System.currentTimeMillis
                            filterTime7 = filterTime7 + (filterEndTime7 - filterBeginTime7).toInt


                            val filterBeginTime8 : Long = System.currentTimeMillis
                            var nextList : Map[Int, Set[Int]] = temMapSec.get(currSnap + (stepNum - 1) * indexSnapInterval).get
                            for (keyid <- nextList.keySet) {
                                var resultSet : Set[Int] = Set[Int]()
                                var firSet : Set[Int] = nextList.get(keyid).get
                                for (i <- firSet) {
                                    if ((resultFlagSec(keyid)(i) == 0) && (resultFlagSec(i)(keyid) == 0)) {
                                        var x = helpMap.get((keyid, i))
                                        if (x != None) {
                                            var count : Int = x.get
                                            helpMap += (keyid, i) -> (count + 1)
                                            if ((count + 1) == stepNum) {
                                                resultSet += i
                                            }
                                        } else {
                                            helpMap += (keyid, i) -> 1
                                        }
                                    }

                                }
                                var x = resultListSec.get((temPartition._3, keyid))
                                if (x != None) {
                                    var mySet : Set[Int] = x.get
                                    mySet ++= resultSet
                                    resultListSec += (temPartition._3, keyid) -> mySet
                                    for (j <- resultSet) {
                                        resultFlagSec(keyid)(j) = 1.toByte
                                    }
                                } else {
                                    var mySet : Set[Int] = Set[Int]()
                                    mySet ++= resultSet
                                    resultListSec += (temPartition._3, keyid) -> mySet
                                    for (j <- resultSet) {
                                        resultFlagSec(keyid)(j) = 1.toByte
                                    }
                                }


                            }
                            val filterEndTime8 : Long = System.currentTimeMillis
                            filterTime8 = filterTime8 + (filterEndTime8 - filterBeginTime8).toInt
                        }
                        lastSnap = currSnap
                        currSnap += indexSnapInterval

                    }
                }
            }
        }

        val filterEndTime : Long = System.currentTimeMillis
        var spaceArray : Array[Int] = Array(iter._3)
        var returnResult : ArrayBuffer[(Int, ArrayBuffer[Int])] = new ArrayBuffer[(Int, ArrayBuffer[Int])]()
        var cache : Map[Int, Array[GPSPoint]] = Map[Int, Array[GPSPoint]]()
        
        val verifyBeginTime : Long = System.currentTimeMillis
        var verifyTime6 : Int = 0
        var verifyTime7 : Int = 0
        var verifyTime8 : Int = 0
        var verifyTime9 : Int = 0
        var verifyTime10 : Int = 0
        var verifyTime11 : Int = 0
        var verifyTime12 : Int = 0
        var time1 : Int = 0
        
        
        var debugCount : Int = 0
        
        var temBeginTime : Long = 0
        var temEndTime   : Long = 0
        var readFileTime : Int = 0
        var decompressTime : Int = 0
        var resultArray : Array[Array[Array[Int]]] = Array.ofDim[Int](trajNumEachSpace, trajNumEachSpace, 3)
        val verifyBeginTime7 : Long = System.currentTimeMillis
        for (i <- 0 until trajNumEachSpace) {
            for (j <- 0 until trajNumEachSpace) {
                if (resultFlag(i)(j) == 1) {
                    resultFlag(j)(i) = 1.toByte
                }
            }
        }

        for (i <- 0 until trajNumEachSpace) {
            for (j <- 0 until trajNumEachSpace) {
                resultArray(i)(j)(0) = -1
                resultArray(i)(j)(1) = -1
                resultArray(i)(j)(2) = -1
            }
        }
        val verifyEndTime7 : Long = System.currentTimeMillis
        verifyTime7 += (verifyEndTime7 - verifyBeginTime7).toInt
        
        for (j <- 0 until spaceArray.length) {
            val verifyBeginTime10 : Long = System.currentTimeMillis
            val spaceID : Int = spaceArray(j)

            val readPath : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + myBaseSettings.trajNumEachSpace.toString + "day" + (dayID).toString + "/day" + (dayID).toString + "_trajic_" + (1440 / myBaseSettings.timePartitionsNum).toString + "/par_ori" + timeID.toString + "zorder" + spaceID.toString + ".tstjs"
            
            temBeginTime = System.currentTimeMillis
            val trajNumData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0, 4)
            temEndTime = System.currentTimeMillis
            readFileTime += (temEndTime - temBeginTime).toInt

            val thisTrajNum : Int = ByteBuffer.wrap(trajNumData).getInt()
            val controlInfoLength : Int = thisTrajNum * 4

            temBeginTime = System.currentTimeMillis
            val testData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0 + 4, controlInfoLength + 4)
            temEndTime = System.currentTimeMillis
            readFileTime += (temEndTime - temBeginTime).toInt
            val verifyEndTime10 : Long = System.currentTimeMillis
            verifyTime10 += (verifyEndTime10 - verifyBeginTime10).toInt
            
            val verifyBeginTime9 : Long = System.currentTimeMillis
            

            // for (keyid <- resultList.keySet) {
            for (keyid <- 0 until (thisTrajNum - 2)) {
                var keyOrderPos : Int = keyid
                var patDataPoints : Array[GPSPoint] = null
                var helpArray : Array[Byte] = new Array[Byte](4)
                for (t <- 0 until 4) {
                    helpArray(t) = testData(keyOrderPos * 4 + t)
                }
                var startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                var stopPos = 0
                
                if (keyOrderPos < (thisTrajNum - 1)) {
                    
                    for (t <- 0 until 4) {
                        helpArray(t) = testData((keyOrderPos + 1) * 4 + t)
                    }
                    stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                    var x = cache.get(keyOrderPos)
                    if (x != None) {
                        patDataPoints = x.get
                        cacheCount += 1
                    } else {
                        temBeginTime = System.currentTimeMillis
                        var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPath, startPos + 4, stopPos + 4)
                        temEndTime = System.currentTimeMillis
                        readFileTime += (temEndTime - temBeginTime).toInt

                        temBeginTime = System.currentTimeMillis
                        patDataPoints = CompressMethod.unTrajicGetPoints(helpArray2)
                        temEndTime = System.currentTimeMillis
                        decompressTime += (temEndTime - temBeginTime).toInt
                        cache += keyOrderPos -> patDataPoints
                    }
                
                    for (k <- (keyid + 1) until (thisTrajNum - 1)) {
                        val myOrderPos : Int = k
                        
                        if ((resultFlag(keyOrderPos)(k) == 1)) {
                            debugCount += 1
                            resultFlag(keyOrderPos)(myOrderPos) = 1.toByte
                            for (t <- 0 until 4) {
                                helpArray(t) = testData(myOrderPos * 4 + t)
                            }
                            startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                            stopPos = 0
                            if (myOrderPos < (thisTrajNum - 1)) {
                                val verifyBeginTime12 : Long = System.currentTimeMillis
                                for (t <- 0 until 4) {
                                    helpArray(t) = testData((myOrderPos + 1) * 4 + t)
                                }
                                stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                                var dataPoints : Array[GPSPoint] = null
                                var x = cache.get(myOrderPos)
                                if (x != None) {
                                    dataPoints = x.get
                                    cacheCount += 1
                                } else {
                                    temBeginTime = System.currentTimeMillis
                                    var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPath, startPos + 4, stopPos + 4)
                                    temEndTime = System.currentTimeMillis
                                    readFileTime += (temEndTime - temBeginTime).toInt

                                    temBeginTime = System.currentTimeMillis
                                    dataPoints = CompressMethod.unTrajicGetPoints(helpArray2)
                                    temEndTime = System.currentTimeMillis
                                    decompressTime += (temEndTime - temBeginTime).toInt

                                    cache += myOrderPos -> dataPoints
                                }
                                var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
                                var flag : Int = 0
                                var temConti : Int = 0
                                var maxConti : Int = 0
                                var pos : Int = 0
                                val verifyEndTime12 : Long = System.currentTimeMillis
                                verifyTime12 += (verifyEndTime12 - verifyBeginTime12).toInt
                                val verifyBeginTime6 : Long = System.currentTimeMillis
                                while(pos < oneParSnapNum){
                                    val curPoint : GPSPoint = dataPoints(pos)
                                    val testLon : Float = curPoint.lon
                                    val testLat : Float = curPoint.lat
                                    val patPoint : GPSPoint = patDataPoints(pos)
                                    val lon : Float = patPoint.lon
                                    val lat : Float = patPoint.lat

                                    val minLon : Float = lon - lonDiff.toFloat
                                    val maxLon : Float = lon + lonDiff.toFloat
                                    val minLat : Float = lat - latDiff.toFloat
                                    val maxLat : Float = lat + latDiff.toFloat


                                    var isLocate : Boolean = false
                                    if ((Math.abs(testLon - (-1.0)) < 0.000001) || (Math.abs(testLat - (-1.0)) < 0.000001) ||
                                                    (Math.abs(lon - (-1.0)) < 0.000001) || (Math.abs(lat - (-1.0)) < 0.000001)){
                                        isLocate = false
                                    }else{
                                        isLocate = PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat,
                                                maxLat, testLon, testLat)
                                    }
                                    if (isLocate){
                                        // totalPairCount += 1
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
                                val verifyEndTime6 : Long = System.currentTimeMillis
                                verifyTime6 += (verifyEndTime6 - verifyBeginTime6).toInt
                                val verifyBeginTime11 : Long = System.currentTimeMillis
                                if (maxConti < temConti){
                                    maxConti = temConti
                                }
                                if (maxConti >= contiSnap){
                                    resultArray(keyOrderPos)(myOrderPos)(0) = 1
                                    resultArray(myOrderPos)(keyOrderPos)(0) = 1
                                    
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
                                        resultArray(keyOrderPos)(myOrderPos)(1) = count
                                        resultArray(myOrderPos)(keyOrderPos)(1) = count
                                        
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
                                        var testFirID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + keyOrderPos * 3 + 2)
                                        var testSecID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + myOrderPos * 3 + 2)
                                        
                                        if (testFirID == 13 && testSecID == 2269 && timeID == 10) {
                                            flag132269 += 1
                                        }
                                        resultArray(keyOrderPos)(myOrderPos)(2) = count
                                        resultArray(myOrderPos)(keyOrderPos)(2) = count
                                        
                                    }
                                }
                                val verifyEndTime11 : Long = System.currentTimeMillis
                                verifyTime11 += (verifyEndTime11 - verifyBeginTime11).toInt
                            }
                        }
                    }
                }

                
            }
            val verifyEndTime9 : Long = System.currentTimeMillis
            verifyTime9 += (verifyEndTime9 - verifyBeginTime9).toInt
            val verifyBeginTime8 : Long = System.currentTimeMillis
            for (i <- 0 until thisTrajNum) {
                var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
                // result += keyid
                var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
                var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
                var rearResult  : ArrayBuffer[Int] = ArrayBuffer[Int]()
                result += timeID + dayID * myBaseSettings.timePartitionsNum
                result += spaceArray(0)
                for (k <- 0 until thisTrajNum) {
                    if (resultArray(i)(k)(0) == 1) {
                        var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + k * 3 + 2)
                        finalResult += t
                    }
                    if (resultArray(i)(k)(1) != -1) {
                        var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + k * 3 + 2)
                        frontResult += t
                        frontResult += resultArray(i)(k)(1)
                    }
                    if (resultArray(i)(k)(2) != -1) {
                        var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + k * 3 + 2)
                        rearResult += t
                        rearResult += resultArray(i)(k)(2)
                    }
                }
                result += finalResult.length
                for (k <- 0 until finalResult.length){
                    result += finalResult(k)
                }
                result += frontResult.length
                for (k <- 0 until frontResult.length){
                    result += frontResult(k)
                }
                result += rearResult.length
                for (k <- 0 until rearResult.length){
                    result += rearResult(k)
                }
                var trajID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + i * 3 + 2)
                
                returnResult += ((trajID, result))
            }
            val verifyEndTime8 : Long = System.currentTimeMillis
            verifyTime8 += (verifyEndTime8 - verifyBeginTime8).toInt

            var path = "/home/" + timeID.toString + ".txt"
            var writer : BufferedWriter = new BufferedWriter(new FileWriter(new File(path)))


            val beginTime1 : Long = System.currentTimeMillis
            for (line <- resultListSec.keySet) {
                var spaceIDSec = line._1
                var testSeq = line._2
                var keyOrderPos = testSeq
                val readPath : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + myBaseSettings.trajNumEachSpace.toString + "day" + (dayID).toString + "/day" + (dayID).toString + "_trajic_" + (1440 / myBaseSettings.timePartitionsNum).toString + "/par_ori" + timeID.toString + "zorder" + spaceID.toString + ".tstjs"
                
                val trajNumData1 : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0, 4)
                val thisTrajNum1 : Int = ByteBuffer.wrap(trajNumData1).getInt()
                val controlInfoLength1 : Int = thisTrajNum1 * 4
                val testData1 : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0 + 4, controlInfoLength1 + 4)
                if (testSeq < thisTrajNum1 - 1) {
                    var patDataPoints : Array[GPSPoint] = null
                    var helpArray : Array[Byte] = new Array[Byte](4)
                    for (t <- 0 until 4) {
                        helpArray(t) = testData1(keyOrderPos * 4 + t)
                    }
                    var startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength1
                    for (t <- 0 until 4) {
                        helpArray(t) = testData1((keyOrderPos + 1) * 4 + t)
                    }
                    var stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength1
                    if (cache.get(testSeq) == None) {
                        var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPath, startPos + 4, stopPos + 4)
                        patDataPoints = CompressMethod.unTrajicGetPoints(helpArray2)
                        cache += keyOrderPos -> patDataPoints
                    } else {
                        patDataPoints = cache.get(testSeq).get
                        cacheCount += 1
                    }

                    val readPathSec : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + myBaseSettings.trajNumEachSpace.toString + "day" + (dayID).toString + "/day" + (dayID).toString + "_trajic_" + (1440 / myBaseSettings.timePartitionsNum).toString + "/par_ori" + timeID.toString + "zorder" + spaceIDSec.toString + ".tstjs"
                    
                    val trajNumData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPathSec, 0, 4)
                    val thisTrajNum : Int = ByteBuffer.wrap(trajNumData).getInt()
                    
                    val controlInfoLength : Int = thisTrajNum * 4
                    val testData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPathSec, 0 + 4, controlInfoLength + 4)
                    var testArraySec = resultListSec.get(line).get
                    totalSecCount += testArraySec.size
                    var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    var rearResult  : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    for (keyid <- testArraySec) {
                        val myOrderPos : Int = keyid
                        for (t <- 0 until 4) {
                            // if ((myOrderPos * 4 + t) < testData.length){
                            //     var u = testData(myOrderPos * 4 + t)
                            //     helpArray(t) = u
                            // } else{
                            //     writer.write(timeID + "\t" + spaceID + "\t" + thisTrajNum + "\t" + testData.length + "\t" + myOrderPos + "\t" + helpArray.length + "\t" + (myOrderPos * 4 + t) + "\n")

                            // }
                            var u = testData(myOrderPos * 4 + t)
                            helpArray(t) = u
                            
                        }
                        var startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                        
                        var stopPos = 0
                        if (myOrderPos < (thisTrajNum - 1)) {
                            val verifyBeginTime12 : Long = System.currentTimeMillis
                            for (t <- 0 until 4) {
                                helpArray(t) = testData((myOrderPos + 1) * 4 + t)
                            }
                            stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                            var dataPoints : Array[GPSPoint] = null
                            var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPathSec, startPos + 4, stopPos + 4)
                            dataPoints = CompressMethod.unTrajicGetPoints(helpArray2)
                            var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
                            var flag : Int = 0
                            var temConti : Int = 0
                            var maxConti : Int = 0
                            var pos : Int = 0
                            val verifyEndTime12 : Long = System.currentTimeMillis
                            verifyTime12 += (verifyEndTime12 - verifyBeginTime12).toInt
                            val verifyBeginTime6 : Long = System.currentTimeMillis
                            while(pos < oneParSnapNum){
                                val curPoint : GPSPoint = dataPoints(pos)
                                val testLon : Float = curPoint.lon
                                val testLat : Float = curPoint.lat
                                val patPoint : GPSPoint = patDataPoints(pos)
                                val lon : Float = patPoint.lon
                                val lat : Float = patPoint.lat  

                                val minLon : Float = lon - lonDiff.toFloat
                                val maxLon : Float = lon + lonDiff.toFloat
                                val minLat : Float = lat - latDiff.toFloat
                                val maxLat : Float = lat + latDiff.toFloat  


                                var isLocate : Boolean = false
                                if ((Math.abs(testLon - (-1.0)) < 0.000001) || (Math.abs(testLat - (-1.0)) < 0.000001) ||
                                                (Math.abs(lon - (-1.0)) < 0.000001) || (Math.abs(lat - (-1.0)) < 0.000001)){
                                    isLocate = false
                                }else{
                                    isLocate = PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat,
                                            maxLat, testLon, testLat)
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
                            val verifyEndTime6 : Long = System.currentTimeMillis
                            verifyTime6 += (verifyEndTime6 - verifyBeginTime6).toInt
                            val verifyBeginTime11 : Long = System.currentTimeMillis
                            if (maxConti < temConti){
                                maxConti = temConti
                            }
                            if (maxConti >= contiSnap){
                                var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + spaceIDSec * myBaseSettings.trajNumEachSpace * 3 + myOrderPos * 3 + 2)
                                finalResult += t
                            }else{
                                if (flagArray(0) == 1){
                                    totalPairCount += 1
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
                                    var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + spaceIDSec * myBaseSettings.trajNumEachSpace * 3 + myOrderPos * 3 + 2)
                                    frontResult += t
                                    frontResult += count

                                }
                                if (flagArray(oneParSnapNum - 1) == 1){
                                    totalPairCount += 1
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
                                    var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + spaceIDSec * myBaseSettings.trajNumEachSpace * 3 + myOrderPos * 3 + 2)
                                    rearResult += t
                                    rearResult += count

                                }
                            }
                            val verifyEndTime11 : Long = System.currentTimeMillis
                            verifyTime11 += (verifyEndTime11 - verifyBeginTime11).toInt
                        }
                    }
                    var trajID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + testSeq * 3 + 2)
                    var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    result += timeID + dayID * myBaseSettings.timePartitionsNum
                    result += spaceArray(0)
                    result += finalResult.length
                    for (k <- 0 until finalResult.length){
                        result += finalResult(k)
                    }
                    result += frontResult.length
                    for (k <- 0 until frontResult.length){
                        result += frontResult(k)
                    }
                    result += rearResult.length
                    for (k <- 0 until rearResult.length){
                        result += rearResult(k)
                    }
                    returnResult += ((trajID, result))

                }
            }
            val endTime1 : Long = System.currentTimeMillis
            time1 += (endTime1 - beginTime1).toInt



        }
        val verifyEndTime : Long = System.currentTimeMillis
        val filterTime : Int = (filterEndTime - filterBeginTime).toInt
        val verifyTime : Int = (verifyEndTime - verifyBeginTime).toInt
        val filterTime1 : Int = (filterEndTime1 - filterBeginTime1).toInt
        val filterTime2 : Int = (filterEndTime2 - filterBeginTime2).toInt
        val filterTime3 : Int = (filterEndTime3 - filterBeginTime3).toInt
        val filterTime4 : Int = (filterEndTime4 - filterBeginTime4).toInt
        returnResult.toArray
    }

    def mapSearchWithIndexRangeQueryWholeJoinFlagUnzip(iter : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]), myBaseSettings : BaseSetting,
                                        bcLookupTable : Broadcast[Array[Array[Int]]]) : Array[(Int, ArrayBuffer[Int])] = {
        var totalPairCount = 0
        var totalSecCount = 0
        var totalInterPar = 0
        var total2 = 0
        var flag132269 = 0
        var cacheCount = 0
        var lookupTable = bcLookupTable.value
        // var indexArray = bcIndexArray.value
        val timePartitionsNum  : Int    = myBaseSettings.timePartitionsNum
        val spacePartitionsNum : Int    = myBaseSettings.spacePartitionsNum
        val contiSnap          : Int    = myBaseSettings.contiSnap
        val totalSnap          : Int    = myBaseSettings.totalSnap
        val delta              : Double = myBaseSettings.delta
        var trajNumEachSpace   : Int    = myBaseSettings.trajNumEachSpace
        
        
        val indexSnapInterval : Int   = myBaseSettings.indexSnapInterval
        val oneParSnapNum     : Int   = myBaseSettings.oneParSnapNum
        val lonGridNum        : Int   = myBaseSettings.lonGridNum
        val latGridNum        : Int   = myBaseSettings.latGridNum
        val MINLON            : Float = myBaseSettings.MINLON
        val MINLAT            : Float = myBaseSettings.MINLAT
        var lonGridLength     : Float = 0.0f
        var latGridLength     : Float = 0.0f

        val dayID  : Int = iter._1
        val timeID : Int = iter._2

        val startSnap : Int = totalSnap / timePartitionsNum * iter._2
        val stopSnap  : Int = totalSnap / timePartitionsNum * (iter._2 + 1)
        val lonDiff   : Double = delta / 111111.0
        val latDiff   : Double = delta / 111111.0
        var temMap    : Map[Int, Map[Int, Set[Int]]] = Map[Int, Map[Int, Set[Int]]]()
        val myIndex   : Array[(MBR, InvertedIndex)] = iter._5
        var currSnap  : Int = startSnap
        

        val filterBeginTime : Long = System.currentTimeMillis
        val filterBeginTime1 : Long = System.currentTimeMillis
        while(currSnap < stopSnap) {
            var temList : Map[Int, Set[Int]] = Map[Int, Set[Int]]()
            val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / indexSnapInterval)
            val myMBR : MBR = curIndex._1
            val myInvertedIndex : InvertedIndex = curIndex._2
            lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
            latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum

            for (i <- 0 until lonGridNum) {
                for (j <- 0 until latGridNum) {
                    var gridID : Int = j * lonGridNum + i
                    if (myInvertedIndex.index(gridID * 2 + 1) != 0) {
                        var minLon : Float = (myMBR.minLon + i * lonGridLength - lonDiff).toFloat
                        var maxLon : Float = (myMBR.minLon + (i + 1) * lonGridLength + lonDiff).toFloat
                        var minLat : Float = (myMBR.minLat + j * latGridLength - latDiff).toFloat
                        var maxLat : Float = (myMBR.minLat + (j + 1) * latGridLength + latDiff).toFloat
                        var minLonGridID : Int = math.floor((minLon - myMBR.minLon) / lonGridLength).toInt
                        var maxLonGridID : Int = math.floor((maxLon - myMBR.minLon) / lonGridLength).toInt
                        var minLatGridID : Int = math.floor((minLat - myMBR.minLat) / latGridLength).toInt
                        var maxLatGridID : Int = math.floor((maxLat - myMBR.minLat) / latGridLength).toInt
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
                        val trajIDOrigin : Set[Int] = Set[Int]()
                        val trajIDExpand : Set[Int] = Set[Int]()
                        
                        for (lonGridID <- minLonGridID to maxLonGridID){
                            for (latGridID <- minLatGridID to maxLatGridID){
                                gridID = latGridID * lonGridNum + lonGridID
                                val beginPos : Int = myInvertedIndex.index(gridID * 2)
                                val length   : Int = myInvertedIndex.index(gridID * 2 + 1)

                                if ((lonGridID == i) && (latGridID == j)) {
                                    for (k <- 0 until length){
                                        trajIDOrigin += myInvertedIndex.idArray(beginPos + k)
                                    }
                                } else {
                                    for (k <- 0 until length){
                                        trajIDExpand += myInvertedIndex.idArray(beginPos + k)
                                    }
                                }
                            }
                        }
                        for (firTrajID <- trajIDOrigin) {
                            var x = temList.get(firTrajID)
                            if (x == None) {
                                var mySet : Set[Int] = Set[Int]()
                                mySet ++= trajIDOrigin
                                mySet ++= trajIDExpand
                                temList += firTrajID -> mySet
                            } else {
                                var mySet : Set[Int] = x.get
                                mySet ++= trajIDOrigin
                                mySet ++= trajIDExpand
                                temList += firTrajID -> mySet
                            }
                        }
                        for (firTrajID <- trajIDExpand) {
                            var x = temList.get(firTrajID)
                            if (x == None) {
                                var mySet : Set[Int] = Set[Int]()
                                mySet ++= trajIDOrigin
                                temList += firTrajID -> mySet
                            } else {
                                var mySet : Set[Int] = x.get
                                mySet ++= trajIDOrigin
                                temList += firTrajID -> mySet
                            }
                        }

                    }
                }
            }
            temMap += currSnap -> temList
            currSnap += indexSnapInterval
        }
        val temMapPath : String = "/home/sigmod/code/" + timeID.toString + ".txt"
        // writeTemMapToFile(temMap, temMapPath)
        val filterEndTime1 : Long = System.currentTimeMillis


        val filterBeginTime2 : Long = System.currentTimeMillis
        var resultList  : Map[Int, Set[Int]] = Map[Int, Set[Int]]()
        var resultFlag : Array[Array[Byte]] = Array.ofDim[Byte](trajNumEachSpace, trajNumEachSpace)
        for (i <- 0 until trajNumEachSpace) {
            for (j <- 0 until trajNumEachSpace) {
                resultFlag(i)(j) = 0
            }
        }
        currSnap = startSnap
        var temList : Map[Int, Set[Int]] = temMap.get(currSnap).get
        for (keyid <- temList.keySet) {
            var firSet : Set[Int] = temList.get(keyid).get
            var x = resultList.get(keyid)
            if (x != None) {
                var mySet : Set[Int] = x.get
                for (i <- firSet) {
                    mySet += i
                    resultFlag(keyid)(i) = 1.toByte
                    resultFlag(i)(keyid) = 1.toByte
                }
                resultList += keyid -> mySet
            } else {
                var mySet : Set[Int] = Set[Int]()
                for (i <- firSet) {
                    mySet += i
                    resultFlag(keyid)(i) = 1.toByte
                    resultFlag(i)(keyid) = 1.toByte
                }
                resultList += keyid -> mySet
            }
        }
        val filterEndTime2 : Long = System.currentTimeMillis

        
        val filterBeginTime4 : Long = System.currentTimeMillis
        currSnap = stopSnap - 1
        temList = Map[Int, Set[Int]]()
        val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / indexSnapInterval)
        val myMBR : MBR = curIndex._1
        val myInvertedIndex : InvertedIndex = curIndex._2
        lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
        latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum
        for (i <- 0 until lonGridNum) {
            for (j <- 0 until latGridNum) {
                var gridID : Int = j * lonGridNum + i
                if (myInvertedIndex.index(gridID * 2 + 1) != 0) {
                    var minLon : Float = (myMBR.minLon + i * lonGridLength - lonDiff).toFloat
                    var maxLon : Float = (myMBR.minLon + (i + 1) * lonGridLength + lonDiff).toFloat
                    var minLat : Float = (myMBR.minLat + j * latGridLength - latDiff).toFloat
                    var maxLat : Float = (myMBR.minLat + (j + 1) * latGridLength + latDiff).toFloat
                    var minLonGridID : Int = math.floor((minLon - myMBR.minLon) / lonGridLength).toInt
                    var maxLonGridID : Int = math.floor((maxLon - myMBR.minLon) / lonGridLength).toInt
                    var minLatGridID : Int = math.floor((minLat - myMBR.minLat) / latGridLength).toInt
                    var maxLatGridID : Int = math.floor((maxLat - myMBR.minLat) / latGridLength).toInt
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
                    val trajIDOrigin : Set[Int] = Set[Int]()
                    val trajIDExpand : Set[Int] = Set[Int]()
                    for (lonGridID <- minLonGridID to maxLonGridID){
                        for (latGridID <- minLatGridID to maxLatGridID){
                            gridID = latGridID * lonGridNum + lonGridID
                            val beginPos : Int = myInvertedIndex.index(gridID * 2)
                            val length   : Int = myInvertedIndex.index(gridID * 2 + 1)
                            if ((lonGridID == i) && (latGridID == j)) {
                                for (k <- 0 until length){
                                    trajIDOrigin += myInvertedIndex.idArray(beginPos + k)
                                }
                            }
                            for (k <- 0 until length){
                                trajIDExpand += myInvertedIndex.idArray(beginPos + k)
                            }

                        }
                    }

                    for (firTrajID <- trajIDOrigin) {
                        var x = temList.get(firTrajID)
                        if (x == None) {
                            var mySet : Set[Int] = Set[Int]()
                            mySet ++= trajIDOrigin
                            mySet ++= trajIDExpand
                            temList += firTrajID -> mySet
                        } else {
                            var mySet : Set[Int] = x.get
                            mySet ++= trajIDOrigin
                            mySet ++= trajIDExpand
                            temList += firTrajID -> mySet
                        }
                    }
                    for (firTrajID <- trajIDExpand) {
                        var x = temList.get(firTrajID)
                        if (x == None) {
                            var mySet : Set[Int] = Set[Int]()
                            mySet ++= trajIDOrigin
                            temList += firTrajID -> mySet
                        } else {
                            var mySet : Set[Int] = x.get
                            mySet ++= trajIDOrigin
                            temList += firTrajID -> mySet
                        }
                    }
                }
                
            
        
            }
        }
        for (keyid <- temList.keySet) {
            var firSet : Set[Int] = temList.get(keyid).get
            var x = resultList.get(keyid)
            if (x != None) {
                var mySet : Set[Int] = x.get
                for (i <- firSet) {
                    
                    if ((resultFlag(keyid)(i) == 0) && (resultFlag(i)(keyid) == 0)) {
                        mySet += i
                        resultFlag(keyid)(i) = 1.toByte
                        resultFlag(i)(keyid) = 1.toByte
                    }
                }
                resultList += keyid -> mySet
            } else {
                var mySet : Set[Int] = Set[Int]()
                for (i <- firSet) {
                    
                    mySet += i
                    resultFlag(keyid)(i) = 1.toByte
                    resultFlag(i)(keyid) = 1.toByte
                }
                resultList += keyid -> mySet
            }
        }
        
        
        val filterEndTime4 : Long = System.currentTimeMillis

        val filterBeginTime3 : Long = System.currentTimeMillis
        var filterTime5 : Int = 0
        var filterTime6 : Int = 0
        var filterTime7 : Int = 0
        var filterTime8 : Int = 0
        val stepNum : Int = math.floor(contiSnap.toDouble / indexSnapInterval).toInt
        var helpMap : Map[(Int, Int), Int] = Map[(Int, Int), Int]()
        var lastSnap : Int = -1
        currSnap = startSnap + indexSnapInterval
        while((currSnap + (stepNum - 1) * indexSnapInterval) < stopSnap) {
            temList = temMap.get(currSnap).get
            if (currSnap == (startSnap + indexSnapInterval)) {
                for (keyid <- temList.keySet) {
                    val filterBeginTime6 : Long = System.currentTimeMillis
                    var firSet : Set[Int] = temList.get(keyid).get
                    for (i <- firSet) {
                        if ((resultFlag(keyid)(i) == 0) && (resultFlag(i)(keyid) == 0)) {
                            helpMap += (keyid, i) -> 1
                        }
                    }
                    val filterEndTime6 : Long = System.currentTimeMillis
                    filterTime6 = filterTime6 + (filterEndTime6 - filterBeginTime6).toInt
                    for (i <- 0 until stepNum) {
                        if (i != 0) {
                            var secMap : Map[Int, Set[Int]] = temMap.get(currSnap + i * indexSnapInterval).get
                            var secList : Set[Int] = Set[Int]()
                            var x = secMap.get(keyid)
                            if (x != None) {
                                secList = x.get
                                val filterBeginTime5 : Long = System.currentTimeMillis
                                for (j <- secList) {
                                    if ((resultFlag(keyid)(j) == 0) && (resultFlag(j)(keyid) == 0)) {
                                        var y = helpMap.get((keyid, j))
                                        if (y != None) {
                                            var count : Int = y.get
                                            helpMap += (keyid, j) -> (count + 1)
                                        } else {
                                            helpMap += (keyid, j) -> 1
                                        }
                                    }
                                    
                                }
                                val filterEndTime5 : Long = System.currentTimeMillis
                                filterTime5 = filterTime5 + (filterEndTime5 - filterBeginTime5).toInt
                            }
                            firSet = firSet intersect secList
                        }
                    }
                    var x = resultList.get(keyid)
                    if (x != None) {
                        var mySet : Set[Int] = x.get
                        mySet ++= firSet
                        resultList += keyid -> mySet
                        for (j <- firSet) {
                            resultFlag(keyid)(j) = 1.toByte
                        }
                    } else {
                        var mySet : Set[Int] = Set[Int]()
                        mySet ++= firSet
                        resultList += keyid -> mySet
                        for (j <- firSet) {
                            resultFlag(keyid)(j) = 1.toByte
                        }
                    }
                }
            } else {
                val filterBeginTime7 : Long = System.currentTimeMillis
                var lastList : Map[Int, Set[Int]] = temMap.get(lastSnap).get
                for (keyid <- lastList.keySet) {
                    var firSet : Set[Int] = lastList.get(keyid).get
                    for (i <- firSet) {
                        if ((resultFlag(keyid)(i) == 0) && (resultFlag(i)(keyid) == 0)) {
                            var x = helpMap.get((keyid, i))
                            if (x != None) {
                                var count : Int = x.get
                                helpMap += (keyid, i) -> (count - 1)
                            }
                        }
                        
                    }
                }
                val filterEndTime7 : Long = System.currentTimeMillis
                filterTime7 = filterTime7 + (filterEndTime7 - filterBeginTime7).toInt


                val filterBeginTime8 : Long = System.currentTimeMillis
                var nextList : Map[Int, Set[Int]] = temMap.get(currSnap + (stepNum - 1) * indexSnapInterval).get
                for (keyid <- nextList.keySet) {
                    var resultSet : Set[Int] = Set[Int]()
                    var firSet : Set[Int] = nextList.get(keyid).get
                    for (i <- firSet) {
                        if ((resultFlag(keyid)(i) == 0) && (resultFlag(i)(keyid) == 0)) {
                            var x = helpMap.get((keyid, i))
                            if (x != None) {
                                var count : Int = x.get
                                helpMap += (keyid, i) -> (count + 1)
                                if ((count + 1) == stepNum) {
                                    resultSet += i
                                }
                            } else {
                                helpMap += (keyid, i) -> 1
                            }
                        }
                        
                    }
                    var x = resultList.get(keyid)
                    if (x != None) {
                        var mySet : Set[Int] = x.get
                        mySet ++= resultSet
                        resultList += keyid -> mySet
                        for (j <- resultSet) {
                            resultFlag(keyid)(j) = 1.toByte
                        }
                    } else {
                        var mySet : Set[Int] = Set[Int]()
                        mySet ++= resultSet
                        resultList += keyid -> mySet
                        for (j <- resultSet) {
                            resultFlag(keyid)(j) = 1.toByte
                        }
                    }
                    

                }
                val filterEndTime8 : Long = System.currentTimeMillis
                filterTime8 = filterTime8 + (filterEndTime8 - filterBeginTime8).toInt
            }
            lastSnap = currSnap
            currSnap += indexSnapInterval
            
        }
        val filterEndTime3 : Long = System.currentTimeMillis


        var myLargeMBRminLon : Float = (iter._4.minLon - lonDiff).toFloat
        var myLargeMBRmaxLon : Float = (iter._4.maxLon + lonDiff).toFloat
        var myLargeMBRminLat : Float = (iter._4.minLat - latDiff).toFloat
        var myLargeMBRmaxLat : Float = (iter._4.maxLat + latDiff).toFloat
        var time2 : Int = 0
        val beginTime2 : Long = System.currentTimeMillis
        var resultListSec  : Map[(Int, Int), Set[Int]] = Map[(Int, Int), Set[Int]]()

        for (i <- 0 until myBaseSettings.spacePartitionsNum) {
            if (i != iter._3) {
                var indexPath : String = "/mnt/data1/billion/" + myBaseSettings.dataset + "/15000/day0/day0_index/time" + iter._2.toString + "space" + i.toString + ".txt"
                var par1 : (MBR, Array[(MBR, InvertedIndex)]) = readIndexFromFilePath(indexPath)
                var temPartition : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]) = (iter._1, iter._2, i, par1._1, par1._2)
                if (temPartition._4.ifIntersectMBR(new MBR(myLargeMBRminLon, myLargeMBRmaxLon, myLargeMBRminLat, myLargeMBRmaxLat))) {
                    total2 += 1
                    var temMapSec : Map[Int, Map[Int, Set[Int]]] = Map[Int, Map[Int, Set[Int]]]()
                    currSnap = startSnap
                    while(currSnap < stopSnap) {
                        var temList : Map[Int, Set[Int]] = Map[Int, Set[Int]]()
                        val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / indexSnapInterval)
                        val secIndex : (MBR, InvertedIndex) = temPartition._5((currSnap - startSnap) / indexSnapInterval)
                        
                        val myMBR : MBR = curIndex._1
                        val myInvertedIndex : InvertedIndex = curIndex._2
                        val secMBR : MBR = secIndex._1
                        val secInvertedIndex : InvertedIndex = secIndex._2
                        
                        lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
                        latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum
                        var lonGridLengthSec = (secMBR.maxLon - secMBR.minLon) / lonGridNum
                        var latGridLengthSec = (secMBR.maxLat - secMBR.minLat) / latGridNum
                        
                        for (i <- 0 until lonGridNum) {
                            for (j <- 0 until latGridNum) {
                                var gridID : Int = j * lonGridNum + i
                                var minLon : Float = (myMBR.minLon + i * lonGridLength - lonDiff).toFloat
                                var maxLon : Float = (myMBR.minLon + (i + 1) * lonGridLength + lonDiff).toFloat
                                var minLat : Float = (myMBR.minLat + j * latGridLength - latDiff).toFloat
                                var maxLat : Float = (myMBR.minLat + (j + 1) * latGridLength + latDiff).toFloat
                                if (secMBR.ifIntersectMBR(new MBR(minLon, maxLon, minLat, maxLat))) {
                                    var minLonGridID : Int = math.floor((minLon - secMBR.minLon) / lonGridLengthSec).toInt
                                    var maxLonGridID : Int = math.floor((maxLon - secMBR.minLon) / lonGridLengthSec).toInt
                                    var minLatGridID : Int = math.floor((minLat - secMBR.minLat) / latGridLengthSec).toInt
                                    var maxLatGridID : Int = math.floor((maxLat - secMBR.minLat) / latGridLengthSec).toInt
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
                                    val trajIDOrigin : Set[Int] = Set[Int]()
                                    val trajIDExpand : Set[Int] = Set[Int]()

                                    for (lonGridID <- minLonGridID to maxLonGridID){
                                        for (latGridID <- minLatGridID to maxLatGridID){
                                            gridID = latGridID * lonGridNum + lonGridID
                                            val beginPos : Int = secInvertedIndex.index(gridID * 2)
                                            val length   : Int = secInvertedIndex.index(gridID * 2 + 1)
                                            for (k <- 0 until length){
                                                trajIDExpand += secInvertedIndex.idArray(beginPos + k)
                                            }
                                        }
                                    }
                                    gridID = j * lonGridNum + i
                                    val beginPos : Int = myInvertedIndex.index(gridID * 2)
                                    val length   : Int = myInvertedIndex.index(gridID * 2 + 1)
                                    for (k <- 0 until length){
                                        trajIDOrigin += myInvertedIndex.idArray(beginPos + k)
                                    }

                                    for (firTrajID <- trajIDOrigin) {
                                        var x = temList.get(firTrajID)
                                        if (x == None) {
                                            var mySet : Set[Int] = Set[Int]()
                                            mySet ++= trajIDExpand
                                            temList += firTrajID -> mySet
                                        } else {
                                            var mySet : Set[Int] = x.get
                                            mySet ++= trajIDExpand
                                            temList += firTrajID -> mySet
                                        }
                                    }
                                }
                            }
                        }

                        
                        temMapSec += currSnap -> temList
                        currSnap += indexSnapInterval
                    }
                    
                    var resultFlagSec : Array[Array[Byte]] = Array.ofDim[Byte](trajNumEachSpace, trajNumEachSpace)
                    for (i <- 0 until trajNumEachSpace) {
                        for (j <- 0 until trajNumEachSpace) {
                            resultFlagSec(i)(j) = 0
                        }
                    }
                    currSnap = startSnap
                    var temListSec : Map[Int, Set[Int]] = temMapSec.get(currSnap).get
                    for (keyid <- temListSec.keySet) {
                        var firSet : Set[Int] = temListSec.get(keyid).get
                        var x = resultListSec.get((temPartition._3, keyid))
                        if (x != None) {
                            var mySet : Set[Int] = x.get
                            for (i <- firSet) {
                                mySet += i
                                resultFlagSec(keyid)(i) = 1.toByte
                                resultFlagSec(i)(keyid) = 1.toByte

                            }
                            resultListSec += (temPartition._3, keyid) -> mySet
                        } else {
                            var mySet : Set[Int] = Set[Int]()
                            for (i <- firSet) {
                                mySet += i
                                resultFlagSec(keyid)(i) = 1.toByte
                                resultFlagSec(i)(keyid) = 1.toByte
                            }
                            resultListSec += (temPartition._3, keyid) -> mySet
                        }
                    }

                    currSnap = stopSnap - 1
                    temListSec = Map[Int, Set[Int]]()
                    val curIndex : (MBR, InvertedIndex) = myIndex((currSnap - startSnap) / indexSnapInterval)
                    val secIndex : (MBR, InvertedIndex) = temPartition._5((currSnap - startSnap) / indexSnapInterval)
                    val myMBR : MBR = curIndex._1
                    val myInvertedIndex : InvertedIndex = curIndex._2
                    val secMBR : MBR = secIndex._1
                    val secInvertedIndex : InvertedIndex = secIndex._2
                    lonGridLength = (myMBR.maxLon - myMBR.minLon) / lonGridNum
                    latGridLength = (myMBR.maxLat - myMBR.minLat) / latGridNum
                    var lonGridLengthSec = (secMBR.maxLon - secMBR.minLon) / lonGridNum
                    var latGridLengthSec = (secMBR.maxLat - secMBR.minLat) / latGridNum
                    for (i <- 0 until lonGridNum) {
                        for (j <- 0 until latGridNum) {
                            var minLon : Float = (myMBR.minLon + i * lonGridLength - lonDiff).toFloat
                            var maxLon : Float = (myMBR.minLon + (i + 1) * lonGridLength + lonDiff).toFloat
                            var minLat : Float = (myMBR.minLat + j * latGridLength - latDiff).toFloat
                            var maxLat : Float = (myMBR.minLat + (j + 1) * latGridLength + latDiff).toFloat
                            if (secMBR.ifIntersectMBR(new MBR(minLon, maxLon, minLat, maxLat))) {
                                var minLonGridID : Int = math.floor((minLon - secMBR.minLon) / lonGridLengthSec).toInt
                                var maxLonGridID : Int = math.floor((maxLon - secMBR.minLon) / lonGridLengthSec).toInt
                                var minLatGridID : Int = math.floor((minLat - secMBR.minLat) / latGridLengthSec).toInt
                                var maxLatGridID : Int = math.floor((maxLat - secMBR.minLat) / latGridLengthSec).toInt
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
                                val trajIDOrigin : Set[Int] = Set[Int]()
                                val trajIDExpand : Set[Int] = Set[Int]()
                                for (lonGridID <- minLonGridID to maxLonGridID){
                                    for (latGridID <- minLatGridID to maxLatGridID){
                                        var gridID : Int = latGridID * lonGridNum + lonGridID
                                        val beginPos : Int = secInvertedIndex.index(gridID * 2)
                                        val length   : Int = secInvertedIndex.index(gridID * 2 + 1)
                                        for (k <- 0 until length){
                                            trajIDExpand += secInvertedIndex.idArray(beginPos + k)
                                        }
                                    }
                                }
                                val gridID : Int = j * lonGridNum + i
                                val beginPos : Int = myInvertedIndex.index(gridID * 2)
                                val length   : Int = myInvertedIndex.index(gridID * 2 + 1)
                                for (k <- 0 until length){
                                    trajIDOrigin += myInvertedIndex.idArray(beginPos + k)
                                }

                                for (firTrajID <- trajIDOrigin) {
                                    var x = temListSec.get(firTrajID)
                                    if (x == None) {
                                        var mySet : Set[Int] = Set[Int]()
                                        mySet ++= trajIDExpand
                                        temListSec += firTrajID -> mySet
                                    } else {
                                        var mySet : Set[Int] = x.get
                                        mySet ++= trajIDExpand
                                        temListSec += firTrajID -> mySet
                                    }
                                }
                            }
                        }
                    }
                    for (keyid <- temListSec.keySet) {
                        var firSet : Set[Int] = temListSec.get(keyid).get
                        var x = resultListSec.get((temPartition._3, keyid))
                        if (x != None) {
                            var mySet : Set[Int] = x.get
                            for (i <- firSet) {
                                var testFirID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + keyid * 3 + 2)
                                var testSecID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + temPartition._3 * myBaseSettings.trajNumEachSpace * 3 + i * 3 + 2)

                                if (testFirID == 13 && testSecID == 2269 && timeID == 10) {
                                    flag132269 += 1
                                }
                                if ((resultFlagSec(keyid)(i) == 0) && (resultFlagSec(i)(keyid) == 0)) {
                                    mySet += i
                                    resultFlagSec(keyid)(i) = 1.toByte
                                    resultFlagSec(i)(keyid) = 1.toByte
                                }
                            }
                            resultListSec += (temPartition._3, keyid) -> mySet
                        } else {
                            var mySet : Set[Int] = Set[Int]()
                            for (i <- firSet) {
                                var testFirID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + keyid * 3 + 2)
                                var testSecID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + temPartition._3 * myBaseSettings.trajNumEachSpace * 3 + i * 3 + 2)

                                if (testFirID == 13 && testSecID == 2269 && timeID == 10) {
                                    flag132269 += 1
                                }
                                mySet += i
                                resultFlagSec(keyid)(i) = 1.toByte
                                resultFlagSec(i)(keyid) = 1.toByte
                            }
                            resultListSec += (temPartition._3, keyid) -> mySet
                        }
                    }
                    var helpMap : Map[(Int, Int), Int] = Map[(Int, Int), Int]()
                    var lastSnap : Int = -1
                    currSnap = startSnap + indexSnapInterval
                    while((currSnap + (stepNum - 1) * indexSnapInterval) < stopSnap) {
                        temListSec = temMapSec.get(currSnap).get
                        if (currSnap == (startSnap + indexSnapInterval)) {
                            for (keyid <- temListSec.keySet) {
                                val filterBeginTime6 : Long = System.currentTimeMillis
                                var firSet : Set[Int] = temListSec.get(keyid).get
                                for (i <- firSet) {
                                    if ((resultFlagSec(keyid)(i) == 0) && (resultFlagSec(i)(keyid) == 0)) {
                                        helpMap += (keyid, i) -> 1
                                    }
                                }
                                val filterEndTime6 : Long = System.currentTimeMillis
                                filterTime6 = filterTime6 + (filterEndTime6 - filterBeginTime6).toInt
                                for (i <- 0 until stepNum) {
                                    if (i != 0) {
                                        var secMap : Map[Int, Set[Int]] = temMapSec.get(currSnap + i * indexSnapInterval).get
                                        var secList : Set[Int] = Set[Int]()
                                        var x = secMap.get(keyid)
                                        if (x != None) {
                                            secList = x.get
                                            val filterBeginTime5 : Long = System.currentTimeMillis
                                            for (j <- secList) {
                                                if ((resultFlagSec(keyid)(j) == 0) && (resultFlagSec(j)(keyid) == 0)) {
                                                    var y = helpMap.get((keyid, j))
                                                    if (y != None) {
                                                        var count : Int = y.get
                                                        helpMap += (keyid, j) -> (count + 1)
                                                    } else {
                                                        helpMap += (keyid, j) -> 1
                                                    }
                                                }

                                            }
                                            val filterEndTime5 : Long = System.currentTimeMillis
                                            filterTime5 = filterTime5 + (filterEndTime5 - filterBeginTime5).toInt
                                        }
                                        firSet = firSet intersect secList
                                    }
                                }
                                var x = resultListSec.get((temPartition._3, keyid))
                                if (x != None) {
                                    var mySet : Set[Int] = x.get
                                    mySet ++= firSet
                                    resultListSec += (temPartition._3, keyid) -> mySet
                                    for (j <- firSet) {
                                        resultFlagSec(keyid)(j) = 1.toByte
                                    }
                                } else {
                                    var mySet : Set[Int] = Set[Int]()
                                    mySet ++= firSet
                                    resultListSec += (temPartition._3, keyid) -> mySet
                                    for (j <- firSet) {
                                        resultFlagSec(keyid)(j) = 1.toByte
                                    }
                                }
                            }
                        } else {
                            val filterBeginTime7 : Long = System.currentTimeMillis
                            var lastList : Map[Int, Set[Int]] = temMapSec.get(lastSnap).get
                            for (keyid <- lastList.keySet) {
                                var firSet : Set[Int] = lastList.get(keyid).get
                                for (i <- firSet) {
                                    if ((resultFlagSec(keyid)(i) == 0) && (resultFlagSec(i)(keyid) == 0)) {
                                        var x = helpMap.get((keyid, i))
                                        if (x != None) {
                                            var count : Int = x.get
                                            helpMap += (keyid, i) -> (count - 1)
                                        }
                                    }

                                }
                            }
                            val filterEndTime7 : Long = System.currentTimeMillis
                            filterTime7 = filterTime7 + (filterEndTime7 - filterBeginTime7).toInt


                            val filterBeginTime8 : Long = System.currentTimeMillis
                            var nextList : Map[Int, Set[Int]] = temMapSec.get(currSnap + (stepNum - 1) * indexSnapInterval).get
                            for (keyid <- nextList.keySet) {
                                var resultSet : Set[Int] = Set[Int]()
                                var firSet : Set[Int] = nextList.get(keyid).get
                                for (i <- firSet) {
                                    if ((resultFlagSec(keyid)(i) == 0) && (resultFlagSec(i)(keyid) == 0)) {
                                        var x = helpMap.get((keyid, i))
                                        if (x != None) {
                                            var count : Int = x.get
                                            helpMap += (keyid, i) -> (count + 1)
                                            if ((count + 1) == stepNum) {
                                                resultSet += i
                                            }
                                        } else {
                                            helpMap += (keyid, i) -> 1
                                        }
                                    }

                                }
                                var x = resultListSec.get((temPartition._3, keyid))
                                if (x != None) {
                                    var mySet : Set[Int] = x.get
                                    mySet ++= resultSet
                                    resultListSec += (temPartition._3, keyid) -> mySet
                                    for (j <- resultSet) {
                                        resultFlagSec(keyid)(j) = 1.toByte
                                    }
                                } else {
                                    var mySet : Set[Int] = Set[Int]()
                                    mySet ++= resultSet
                                    resultListSec += (temPartition._3, keyid) -> mySet
                                    for (j <- resultSet) {
                                        resultFlagSec(keyid)(j) = 1.toByte
                                    }
                                }


                            }
                            val filterEndTime8 : Long = System.currentTimeMillis
                            filterTime8 = filterTime8 + (filterEndTime8 - filterBeginTime8).toInt
                        }
                        lastSnap = currSnap
                        currSnap += indexSnapInterval

                    }
                }
            }
        }


        
        val filterEndTime : Long = System.currentTimeMillis
        var spaceArray : Array[Int] = Array(iter._3)
        var returnResult : ArrayBuffer[(Int, ArrayBuffer[Int])] = new ArrayBuffer[(Int, ArrayBuffer[Int])]()
        var cache : Map[Int, Array[GPSPoint]] = Map[Int, Array[GPSPoint]]()
        
        val verifyBeginTime : Long = System.currentTimeMillis
        var verifyTime6 : Int = 0
        var verifyTime7 : Int = 0
        var verifyTime8 : Int = 0
        var verifyTime9 : Int = 0
        var verifyTime10 : Int = 0
        var verifyTime11 : Int = 0
        var verifyTime12 : Int = 0
        var time1 : Int = 0
        
        
        var debugCount : Int = 0
        
        var temBeginTime : Long = 0
        var temEndTime   : Long = 0
        var readFileTime : Int = 0
        var decompressTime : Int = 0
        var resultArray : Array[Array[Array[Int]]] = Array.ofDim[Int](trajNumEachSpace, trajNumEachSpace, 3)
        val verifyBeginTime7 : Long = System.currentTimeMillis
        for (i <- 0 until trajNumEachSpace) {
            for (j <- 0 until trajNumEachSpace) {
                if (resultFlag(i)(j) == 1) {
                    resultFlag(j)(i) = 1.toByte
                }
            }
        }

        for (i <- 0 until trajNumEachSpace) {
            for (j <- 0 until trajNumEachSpace) {
                resultArray(i)(j)(0) = -1
                resultArray(i)(j)(1) = -1
                resultArray(i)(j)(2) = -1
            }
        }
        val verifyEndTime7 : Long = System.currentTimeMillis
        verifyTime7 += (verifyEndTime7 - verifyBeginTime7).toInt
        
        for (j <- 0 until spaceArray.length) {
            val verifyBeginTime10 : Long = System.currentTimeMillis
            val spaceID : Int = spaceArray(j)

            // porto
            // val readPath : String = "/mnt/data3/changzhihao/billion/porto/100000/day0-time-space-test/day" + dayID.toString + "Zorder-60min/day" + dayID.toString + "-trajic-60-4000/par_ori" + timeID.toString + "zorder" + spaceID.toString + ".tstjs"
            // singapore
            // val readPath : String = "/mnt/data3/changzhihao/billion/singapore/15000/day" + dayID.toString + "Zorder-60min/day" + dayID.toString + "-trajic-60-4000/par_ori" + timeID.toString + "zorder" + spaceID.toString + ".tstjs"
            val readPath : String = "/mnt/data1/billion/" + myBaseSettings.dataset + "/15000/day" + dayID.toString + "/day" + dayID.toString + "_zorder/par_ori" + timeID.toString + "zorder" + spaceID.toString + ".tstjs"
            
            temBeginTime = System.currentTimeMillis
            // val trajNumData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0, 4)
            temEndTime = System.currentTimeMillis
            readFileTime += (temEndTime - temBeginTime).toInt

            var thisTrajNum : Int = 4000
            if (iter._3 == 3) {
                thisTrajNum = 3000
            }
            // val thisTrajNum : Int = ByteBuffer.wrap(trajNumData).getInt()
            // val controlInfoLength : Int = thisTrajNum * 4

            temBeginTime = System.currentTimeMillis
            // val testData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0 + 4, controlInfoLength + 4)
            temEndTime = System.currentTimeMillis
            readFileTime += (temEndTime - temBeginTime).toInt
            val verifyEndTime10 : Long = System.currentTimeMillis
            verifyTime10 += (verifyEndTime10 - verifyBeginTime10).toInt
            
            val verifyBeginTime9 : Long = System.currentTimeMillis
            

            // for (keyid <- resultList.keySet) {
            for (keyid <- 0 until (thisTrajNum - 2)) {
                var keyOrderPos : Int = keyid
                var patDataPoints : Array[GPSPoint] = null
                var helpArray : Array[Byte] = new Array[Byte](4)
                // for (t <- 0 until 4) {
                //     helpArray(t) = testData(keyOrderPos * 4 + t)
                // }
                
                // var startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                var startPos = 720 * 8 * keyOrderPos
                var stopPos = 720 * 8 * (keyOrderPos + 1)
                
                if (keyOrderPos < (thisTrajNum - 1)) {
                    
                    // for (t <- 0 until 4) {
                    //     helpArray(t) = testData((keyOrderPos + 1) * 4 + t)
                    // }
                    // stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                    var x = cache.get(keyOrderPos)
                    if (x != None) {
                        patDataPoints = x.get
                        cacheCount += 1
                    } else {
                        temBeginTime = System.currentTimeMillis
                        var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPath, startPos, stopPos)
                        temEndTime = System.currentTimeMillis
                        readFileTime += (temEndTime - temBeginTime).toInt

                        temBeginTime = System.currentTimeMillis
                        patDataPoints = getPoints(helpArray2)
                        temEndTime = System.currentTimeMillis
                        decompressTime += (temEndTime - temBeginTime).toInt
                        cache += keyOrderPos -> patDataPoints
                    }
                    // var temArrayBuffer : Array[Int] = resultList.get(keyid).get.toArray
                    
                    // for (k <- 0 until temArrayBuffer.length) {
                    for (k <- (keyid + 1) until (thisTrajNum - 1)) {
                        // val myOrderPos : Int = temArrayBuffer(k)
                        val myOrderPos : Int = k
                        
                        if ((resultFlag(keyOrderPos)(k) == 1)) {
                            debugCount += 1
                            resultFlag(keyOrderPos)(myOrderPos) = 1.toByte
                            // for (t <- 0 until 4) {
                            //     helpArray(t) = testData(myOrderPos * 4 + t)
                            // }
                            startPos = 720 * 8 * myOrderPos
                            stopPos = 720 * 8 * (myOrderPos + 1)
                            if (myOrderPos < (thisTrajNum - 1)) {
                                val verifyBeginTime12 : Long = System.currentTimeMillis
                                // for (t <- 0 until 4) {
                                //     helpArray(t) = testData((myOrderPos + 1) * 4 + t)
                                // }
                                // stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                                var dataPoints : Array[GPSPoint] = null
                                var x = cache.get(myOrderPos)
                                if (x != None) {
                                    dataPoints = x.get
                                    cacheCount += 1
                                } else {
                                    temBeginTime = System.currentTimeMillis
                                    var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPath, startPos, stopPos)
                                    temEndTime = System.currentTimeMillis
                                    readFileTime += (temEndTime - temBeginTime).toInt

                                    temBeginTime = System.currentTimeMillis
                                    dataPoints = getPoints(helpArray2)
                                    temEndTime = System.currentTimeMillis
                                    decompressTime += (temEndTime - temBeginTime).toInt

                                    cache += myOrderPos -> dataPoints
                                }
                                var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
                                var flag : Int = 0
                                var temConti : Int = 0
                                var maxConti : Int = 0
                                var pos : Int = 0
                                val verifyEndTime12 : Long = System.currentTimeMillis
                                verifyTime12 += (verifyEndTime12 - verifyBeginTime12).toInt
                                val verifyBeginTime6 : Long = System.currentTimeMillis
                                while(pos < oneParSnapNum){
                                    val curPoint : GPSPoint = dataPoints(pos)
                                    val testLon : Float = curPoint.lon
                                    val testLat : Float = curPoint.lat
                                    val patPoint : GPSPoint = patDataPoints(pos)
                                    val lon : Float = patPoint.lon
                                    val lat : Float = patPoint.lat

                                    val minLon : Float = lon - lonDiff.toFloat
                                    val maxLon : Float = lon + lonDiff.toFloat
                                    val minLat : Float = lat - latDiff.toFloat
                                    val maxLat : Float = lat + latDiff.toFloat


                                    var isLocate : Boolean = false
                                    if ((Math.abs(testLon - (-1.0)) < 0.000001) || (Math.abs(testLat - (-1.0)) < 0.000001) ||
                                                    (Math.abs(lon - (-1.0)) < 0.000001) || (Math.abs(lat - (-1.0)) < 0.000001)){
                                        isLocate = false
                                    }else{
                                        isLocate = PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat,
                                                maxLat, testLon, testLat)
                                    }
                                    if (isLocate){
                                        // totalPairCount += 1
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
                                val verifyEndTime6 : Long = System.currentTimeMillis
                                verifyTime6 += (verifyEndTime6 - verifyBeginTime6).toInt
                                val verifyBeginTime11 : Long = System.currentTimeMillis
                                if (maxConti < temConti){
                                    maxConti = temConti
                                }
                                if (maxConti >= contiSnap){
                                    resultArray(keyOrderPos)(myOrderPos)(0) = 1
                                    resultArray(myOrderPos)(keyOrderPos)(0) = 1
                                    
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
                                        resultArray(keyOrderPos)(myOrderPos)(1) = count
                                        resultArray(myOrderPos)(keyOrderPos)(1) = count
                                        
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
                                        var testFirID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + keyOrderPos * 3 + 2)
                                        var testSecID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + myOrderPos * 3 + 2)
                                        
                                        if (testFirID == 13 && testSecID == 2269 && timeID == 10) {
                                            flag132269 += 1
                                        }
                                        resultArray(keyOrderPos)(myOrderPos)(2) = count
                                        resultArray(myOrderPos)(keyOrderPos)(2) = count
                                        
                                    }
                                }
                                val verifyEndTime11 : Long = System.currentTimeMillis
                                verifyTime11 += (verifyEndTime11 - verifyBeginTime11).toInt
                            }
                        }
                    }
                }

                
            }
            val verifyEndTime9 : Long = System.currentTimeMillis
            verifyTime9 += (verifyEndTime9 - verifyBeginTime9).toInt
            val verifyBeginTime8 : Long = System.currentTimeMillis
            for (i <- 0 until thisTrajNum) {
                var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
                // result += keyid
                var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
                var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
                var rearResult  : ArrayBuffer[Int] = ArrayBuffer[Int]()
                result += timeID + dayID * myBaseSettings.timePartitionsNum
                result += spaceArray(0)
                for (k <- 0 until thisTrajNum) {
                    if (resultArray(i)(k)(0) == 1) {
                        var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + k * 3 + 2)
                        finalResult += t
                    }
                    if (resultArray(i)(k)(1) != -1) {
                        var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + k * 3 + 2)
                        frontResult += t
                        frontResult += resultArray(i)(k)(1)
                    }
                    if (resultArray(i)(k)(2) != -1) {
                        var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + k * 3 + 2)
                        rearResult += t
                        rearResult += resultArray(i)(k)(2)
                    }
                }
                result += finalResult.length
                for (k <- 0 until finalResult.length){
                    result += finalResult(k)
                }
                result += frontResult.length
                for (k <- 0 until frontResult.length){
                    result += frontResult(k)
                }
                result += rearResult.length
                for (k <- 0 until rearResult.length){
                    result += rearResult(k)
                }
                var trajID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + i * 3 + 2)
                // if (trajID == 0) {
                //     returnResult += ((trajID, result))
                // }
                returnResult += ((trajID, result))
            }
            val verifyEndTime8 : Long = System.currentTimeMillis
            verifyTime8 += (verifyEndTime8 - verifyBeginTime8).toInt

            val beginTime1 : Long = System.currentTimeMillis
            for (line <- resultListSec.keySet) {
                var spaceIDSec = line._1
                var testSeq = line._2
                var keyOrderPos = testSeq
                val readPath : String = "/mnt/data1/billion/" + myBaseSettings.dataset + "/15000/day" + dayID.toString + "/day" + dayID.toString + "_zorder/par_ori" + timeID.toString + "zorder" + spaceID.toString + ".tstjs"
                // val trajNumData1 : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0, 4)
                // val thisTrajNum1 : Int = ByteBuffer.wrap(trajNumData1).getInt()
                // val controlInfoLength1 : Int = thisTrajNum1 * 4
                // val testData1 : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPath, 0 + 4, controlInfoLength1 + 4)
                var thisTrajNum1 : Int = 4000
                if (iter._3 == 3) {
                    thisTrajNum1 = 3000
                }
                if (testSeq < thisTrajNum1 - 1) {
                    var patDataPoints : Array[GPSPoint] = null
                    var helpArray : Array[Byte] = new Array[Byte](4)
                    // for (t <- 0 until 4) {
                    //     helpArray(t) = testData1(keyOrderPos * 4 + t)
                    // }
                    // var startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength1
                    // for (t <- 0 until 4) {
                    //     helpArray(t) = testData1((keyOrderPos + 1) * 4 + t)
                    // }
                    // var stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength1
                    var startPos = 720 * 8 * keyOrderPos
                    var stopPos = 720 * 8 * (keyOrderPos + 1)
                    if (cache.get(testSeq) == None) {
                        var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPath, startPos, stopPos)
                        patDataPoints = getPoints(helpArray2)
                        cache += keyOrderPos -> patDataPoints
                    } else {
                        patDataPoints = cache.get(testSeq).get
                        cacheCount += 1
                    }

                    val readPathSec : String = "/mnt/data1/billion/" + myBaseSettings.dataset + "/15000/day" + dayID.toString + "/day" + dayID.toString + "_zorder/par_ori" + timeID.toString + "zorder" + spaceIDSec.toString + ".tstjs"
                    // val trajNumData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPathSec, 0, 4)
                    // val thisTrajNum : Int = ByteBuffer.wrap(trajNumData).getInt()
                    // val controlInfoLength : Int = thisTrajNum * 4
                    // val testData : Array[Byte] = MethodCommonFunction.readByteRandomAccess(readPathSec, 0 + 4, controlInfoLength + 4)
                    var thisTrajNum : Int = 4000
                    if (line._1 == 3) {
                        thisTrajNum = 3000
                    }
                    var testArraySec = resultListSec.get(line).get
                    totalSecCount += testArraySec.size
                    var finalResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    var frontResult : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    var rearResult  : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    for (keyid <- testArraySec) {
                        val myOrderPos : Int = keyid
                        // for (t <- 0 until 4) {
                        //     helpArray(t) = testData(myOrderPos * 4 + t)
                        // }
                        // var startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                        // var stopPos = 0
                        if (myOrderPos < (thisTrajNum - 1)) {
                            val verifyBeginTime12 : Long = System.currentTimeMillis
                            // for (t <- 0 until 4) {
                            //     helpArray(t) = testData((myOrderPos + 1) * 4 + t)
                            // }
                            // stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
                            var dataPoints : Array[GPSPoint] = null
                            var startPos = 720 * 8 * myOrderPos
                            var stopPos = 720 * 8 * (myOrderPos + 1)
                            var helpArray2 = MethodCommonFunction.readByteRandomAccess(readPathSec, startPos, stopPos)
                            dataPoints = getPoints(helpArray2)
                            var flagArray : Array[Int] = new Array[Int](oneParSnapNum)
                            var flag : Int = 0
                            var temConti : Int = 0
                            var maxConti : Int = 0
                            var pos : Int = 0
                            val verifyEndTime12 : Long = System.currentTimeMillis
                            verifyTime12 += (verifyEndTime12 - verifyBeginTime12).toInt
                            val verifyBeginTime6 : Long = System.currentTimeMillis
                            while(pos < oneParSnapNum){
                                val curPoint : GPSPoint = dataPoints(pos)
                                val testLon : Float = curPoint.lon
                                val testLat : Float = curPoint.lat
                                val patPoint : GPSPoint = patDataPoints(pos)
                                val lon : Float = patPoint.lon
                                val lat : Float = patPoint.lat  

                                val minLon : Float = lon - lonDiff.toFloat
                                val maxLon : Float = lon + lonDiff.toFloat
                                val minLat : Float = lat - latDiff.toFloat
                                val maxLat : Float = lat + latDiff.toFloat  


                                var isLocate : Boolean = false
                                if ((Math.abs(testLon - (-1.0)) < 0.000001) || (Math.abs(testLat - (-1.0)) < 0.000001) ||
                                                (Math.abs(lon - (-1.0)) < 0.000001) || (Math.abs(lat - (-1.0)) < 0.000001)){
                                    isLocate = false
                                }else{
                                    isLocate = PublicFunc.ifLocateSafeArea(minLon, maxLon, minLat,
                                            maxLat, testLon, testLat)
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
                            val verifyEndTime6 : Long = System.currentTimeMillis
                            verifyTime6 += (verifyEndTime6 - verifyBeginTime6).toInt
                            val verifyBeginTime11 : Long = System.currentTimeMillis
                            if (maxConti < temConti){
                                maxConti = temConti
                            }
                            if (maxConti >= contiSnap){
                                var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + spaceIDSec * myBaseSettings.trajNumEachSpace * 3 + myOrderPos * 3 + 2)
                                finalResult += t
                            }else{
                                if (flagArray(0) == 1){
                                    totalPairCount += 1
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
                                    var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + spaceIDSec * myBaseSettings.trajNumEachSpace * 3 + myOrderPos * 3 + 2)
                                    frontResult += t
                                    frontResult += count

                                }
                                if (flagArray(oneParSnapNum - 1) == 1){
                                    totalPairCount += 1
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
                                    var t = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + spaceIDSec * myBaseSettings.trajNumEachSpace * 3 + myOrderPos * 3 + 2)
                                    rearResult += t
                                    rearResult += count

                                }
                            }
                            val verifyEndTime11 : Long = System.currentTimeMillis
                            verifyTime11 += (verifyEndTime11 - verifyBeginTime11).toInt
                        }
                    }
                    var trajID : Int = lookupTable(iter._1)(iter._2 * myBaseSettings.totalTrajNums * 3 + iter._3 * myBaseSettings.trajNumEachSpace * 3 + testSeq * 3 + 2)
                    var result : ArrayBuffer[Int] = ArrayBuffer[Int]()
                    result += timeID + dayID * myBaseSettings.timePartitionsNum
                    result += spaceArray(0)
                    result += finalResult.length
                    for (k <- 0 until finalResult.length){
                        result += finalResult(k)
                    }
                    result += frontResult.length
                    for (k <- 0 until frontResult.length){
                        result += frontResult(k)
                    }
                    result += rearResult.length
                    for (k <- 0 until rearResult.length){
                        result += rearResult(k)
                    }
                    returnResult += ((trajID, result))

                }
            }
            val endTime1 : Long = System.currentTimeMillis
            time1 += (endTime1 - beginTime1).toInt



        }
        val verifyEndTime : Long = System.currentTimeMillis
        val filterTime : Int = (filterEndTime - filterBeginTime).toInt
        val verifyTime : Int = (verifyEndTime - verifyBeginTime).toInt
        val filterTime1 : Int = (filterEndTime1 - filterBeginTime1).toInt
        val filterTime2 : Int = (filterEndTime2 - filterBeginTime2).toInt
        val filterTime3 : Int = (filterEndTime3 - filterBeginTime3).toInt
        val filterTime4 : Int = (filterEndTime4 - filterBeginTime4).toInt
        // returnResult += ((-1, ArrayBuffer[Int](filterTime, verifyTime, time1, time2, cacheCount)))
        // returnResult += ((-1, ArrayBuffer[Int](filterTime, verifyTime, readFileTime, decompressTime, filterTime1, filterTime2, filterTime3, filterTime4, filterTime5, filterTime6, filterTime7, filterTime8, verifyTime6, verifyTime7, verifyTime8, verifyTime9, verifyTime10, verifyTime11, verifyTime12, debugCount)))
        returnResult.toArray
    }

    def getFinalResultWholeJoin(inputArray : Array[Array[Int]], myBaseSettings : BaseSetting, bcLookupTable : Broadcast[Array[Array[Int]]]) : Array[Int] = {
        var finalResult : Set[Int] = Set[Int]()
        var temMap : Map[Int, Int] = Map[Int, Int]()
        var continuousFlag : Boolean = false
        var arrayMap : Map[Int, ArrayBuffer[Array[Int]]] = MethodCommonFunction.simplificationArrayInTimeWhole(inputArray)
        var lookupTable = bcLookupTable.value
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
                                val canTrajID : Int = trajPos
                                // val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
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
                                val canTrajID : Int = trajPos
                                // val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
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
                                val canTrajID : Int = trajPos
                                // val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
                                if (!finalResult.contains(canTrajID)){
                                    finalResult.add(canTrajID)
                                }
                            }
                            pos = pos + length + 1
                            length = temArray(pos)
                            myArray = temArray.slice(pos + 1, pos + length + 1)
                            for (k <- 0 until myArray.length / 2){
                                val trajPos : Int = myArray(k * 2)
                                val canTrajID : Int = trajPos
                                // val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
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
                                val canTrajID : Int = trajPos
                                // val canTrajID : Int = lookupTable(dayID)(timeID * myBaseSettings.totalTrajNums * 3 + spaceID * myBaseSettings.trajNumEachSpace * 3 + trajPos * 3 + 2)
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

    def readIndexFromFilePath(path : String) : (MBR, Array[(MBR, InvertedIndex)]) = {
        var in : DataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))
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

    def getPoints(data : Array[Byte]) : Array[GPSPoint] = {
        var points : Array[GPSPoint] = new Array[GPSPoint](720)
        for (i <- 0 until 720) {
            var pos = i * 8
            var helpArray1 : Array[Byte] = new Array[Byte](4)
            helpArray1(0) = data(pos + 0)
            helpArray1(1) = data(pos + 1)
            helpArray1(2) = data(pos + 2)
            helpArray1(3) = data(pos + 3)
            var lon : Float = ByteBuffer.wrap(helpArray1).getFloat()
            helpArray1(0) = data(pos + 4)
            helpArray1(1) = data(pos + 5)
            helpArray1(2) = data(pos + 6)
            helpArray1(3) = data(pos + 7)
            var lat : Float = ByteBuffer.wrap(helpArray1).getFloat()
            
            var point : GPSPoint = new GPSPoint(0.0.toFloat, lon, lat)
            points(i) = point
            
        }
        points
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
