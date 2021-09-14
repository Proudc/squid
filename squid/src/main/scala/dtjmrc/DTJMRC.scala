package src.main.scala.squmr

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
import src.main.scala.squid.MethodCommonFunction

object DTJMRC {
    
    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .config("spark.files.maxPartitionBytes", 128 * 1024)
                    .config("spark.driver.maxResultSize", "8g")
                    .appName("DTJMRC-Join")
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
        // myBaseSettings.setRootPath("file:///mnt/data1/billion/porto/15000/")
        myBaseSettings.setRootPath("file:///mnt/data1/trillion/porto/")


        // paramater change 4, days num
        myBaseSettings.setDaysNum(1)
        
        // paramater change 5, time partitions num
        myBaseSettings.setTimePartitionsNum(48)
        
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
        myBaseSettings.setDataset("porto")

        // paramater change 12, whether to write the index to the file，1 is write and 0 is not write
        myBaseSettings.setWriteIndexToFileFlag(1)

        // paramater change 13, set the source of the read index, "zip" or "file"
        myBaseSettings.setReadIndexFromWhere("file")

        var datasetList : Array[String] = Array("singapore", "porto")
        var totalTrajNumList : Array[Int] = Array(10000, 10000, 12500, 15000, 17500, 20000)
        var deltaList : Array[Double] = Array(50, 50, 100, 150, 200, 250)
        var contiSnapList : Array[Int] = Array(120, 120, 180, 240, 300, 360)
        var daysNumList : Array[Int] = Array(0, 1, 2, 3)

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

        val inputFilePath       : Array[String] = MethodCommonFunction.getInitialInputFilePathWhole(myBaseSettings, "day", "/day", "_trajic/par_ori", "zorder", ".tstjs")
        val lookupTableFilePath : Array[String] = MethodCommonFunction.getLookupTableFilePathWhole(myBaseSettings, "day", "/day", "_zorder/par_map", ".tstjs")
        var lookupTable     : Array[Array[Int]] = MethodCommonFunction.getLookupTableWhole(sc, lookupTableFilePath, myBaseSettings)
        val bcLookupTable   : Broadcast[Array[Array[Int]]] = sc.broadcast(lookupTable)

        val time1 : Long = System.currentTimeMillis
        val dataRDD : RDD[(Int, Int, Int, Array[(Int, Int, Float, Float)])] = getMetaDataFromTrajic(sc, inputFilePath, myBaseSettings, bcLookupTable)
        val time2 : Long = System.currentTimeMillis
        println("The time to get dataRDD is: " + ((time2 - time1) / 1000.0).toDouble)
        doSearchEntryJoin(sc, myBaseSettings, dataRDD, bcLookupTable)

    }

    def doSearchEntryJoin(sc : SparkContext, myBaseSettings : BaseSetting, dataRDD : RDD[(Int, Int, Int, Array[(Int, Int, Float, Float)])],
                                        bcLookupTable : Broadcast[Array[Array[Int]]]) : Unit = {
        
        val time1 : Long = System.currentTimeMillis
        val gridNum : Int = 100
        var tempA  : Array[(Int, Int)] = dataRDD.map(l => join(l, myBaseSettings, gridNum, bcLookupTable))
                                                .flatMap(l => l)
                                                       .partitionBy(new keyOfPartitioner(myBaseSettings.totalTrajNums))
                                                       .mapPartitions(l => refine(l, myBaseSettings.contiSnap))
                                                       .collect()
        val time2 : Long = System.currentTimeMillis
        val totalJoinTime : Double = ((time2 - time1) / 1000.0).toDouble

        println("\n----------------------------------------------------------")
        println("Join time: " + totalJoinTime)
        println("The number of close contacts is: " + tempA.length)
        println("----------------------------------------------------------")
        // val queryResultRootWritePath : String = "/home/changzhihao/sigmod/code/python/sigmod_exper/squmr_" + myBaseSettings.dataset + "_" + myBaseSettings.daysNum.toString + "0.5_"
        // val queryResultWritePath : String = queryResultRootWritePath + myBaseSettings.totalTrajNums.toString + "_" + myBaseSettings.delta.toInt.toString + "_" + myBaseSettings.contiSnap.toString + ".txt"
        // var writer : BufferedWriter = new BufferedWriter(new FileWriter(new File(queryResultWritePath)))
        // writer.write(totalJoinTime.toString + "\t" + tempA.length.toString)
        // writer.close()
    }

    
    def getMetaDataFromTrajic(sc : SparkContext, inputFilePath : Array[String], myBaseSettings : BaseSetting,
                                                           bcLookupTable : Broadcast[Array[Array[Int]]]) : RDD[(Int, Int, Int, Array[(Int, Int, Float, Float)])] = {
        val inputRDD1 = sc.binaryFiles(inputFilePath.mkString(","))
        val inputRDD : RDD[(Int, Int, Int, Array[(Int, Int, Float, Float)])] = inputRDD1.map(l => (l._1, l._2.toArray))
                                                                       .map(l => getMetaDataFromTrajicRunner(l, myBaseSettings, bcLookupTable))
        
        inputRDD                                      
    }

    def getMetaDataFromTrajicRunner(iter1 : (String, Array[Byte]), myBaseSettings : BaseSetting,
                                bcLookupTable : Broadcast[Array[Array[Int]]]) : (Int, Int, Int, Array[(Int, Int, Float, Float)]) = {
        var lookupTable = bcLookupTable.value
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
        }else{
            var firStr = line.slice(7, 10)
            var secStr = line.slice(16, line.length)
            timeID = firStr.toInt
            spaceID = secStr.toInt
        }
        // billion
        // val lines3 : String = lines1(lines1.length - 3)
        // trillion
        val lines3 : String = lines1(lines1.length - 2)
        dayID = lines3.slice(3, 4).toInt

        var zipData : Array[Byte] = iter1._2
        
        var result = CompressMethod.unTrajicTotalFile(zipData, dayID, timeID, spaceID, lookupTable, myBaseSettings)
        
        (dayID, timeID, spaceID, result.toArray)

    }

    def ifLocateSafeArea(D1 : (Int, Int, Float, Float), D2 : (Int, Int, Float, Float), delta : Double) : Boolean = {
        var lonDiff : Float = (delta / 111111.0).toFloat
        var latDiff : Float = (delta / 111111.0).toFloat
        var minLon : Float = D1._3 - lonDiff
        var maxLon : Float = D1._3 + lonDiff
        var minLat : Float = D1._4 - latDiff
        var maxLat : Float = D1._4 + latDiff
        if ((D2._3 >= minLon) && (D2._3 <= maxLon) && (D2._4 >= minLat) && (D2._4 <= maxLat)) {
            return true
        } else {
            return false
        }
    }

    def join(iter : (Int, Int, Int, Array[(Int, Int, Float, Float)]), myBaseSettings : BaseSetting,
                                        gridNum : Int, bcLookupTable : Broadcast[Array[Array[Int]]]) : ArrayBuffer[(Int, (Int, Int))] = {
        var lookupTable = bcLookupTable.value
        val partitionID : Int = TaskContext.get.partitionId
        var path = "/home/squmr-join-" + myBaseSettings.dataset + "-" + partitionID.toString + ".txt"
        var writer : BufferedWriter = new BufferedWriter(new FileWriter(new File(path)))
        
        val minLon : Float = myBaseSettings.MINLON
        val maxLon : Float = myBaseSettings.MAXLON
        val minLat : Float = myBaseSettings.MINLAT
        val maxLat : Float = myBaseSettings.MAXLAT
        val delta : Double = myBaseSettings.delta
        val lonDiff : Double = delta / 111111.0
        val latDiff : Double = delta / 111111.0
        val lonGridLength : Float = (maxLon - minLon) / gridNum
        val latGridLength : Float = (maxLat - minLat) / gridNum

        var currMinLon : Float = Float.MaxValue
        var currMaxLon : Float = Float.MinValue
        var currMinLat : Float = Float.MaxValue
        var currMaxLat : Float = Float.MinValue

        var JP : ArrayBuffer[(Int, (Int, Int))] = new ArrayBuffer[(Int, (Int, Int))]
        var spi : Array[Array[ArrayBuffer[(Int, Int, Float, Float)]]] = Array.ofDim[ArrayBuffer[(Int, Int, Float, Float)]](gridNum * gridNum, 17280)
        
        var dayID   : Int = iter._1
        var timeID  : Int = iter._2
        var spaceID : Int = iter._3

        var total_time1 : Double = 0.0
        var total_time2 : Double = 0.0
        var total_time3 : Double = 0.0
        var total_time4 : Double = 0.0
        var total_time5 : Double = 0.0
        var total_time6 : Double = 0.0

        var pos : Int = 0
        
        for (i <- iter._4) {
            pos += 1
            if (i._4 != -1) {
                val time1 : Long = System.currentTimeMillis
                val lon : Float = i._3
                val lat : Float = i._4
                currMinLon = math.min(currMinLon, lon)
                currMaxLon = math.max(currMaxLon, lon)
                currMinLat = math.min(currMinLat, lat)
                currMaxLat = math.max(currMaxLat, lat)
                var lonGridID : Int = floor((lon - minLon) / lonGridLength).toInt
                var latGridID : Int = floor((lat - minLat) / latGridLength).toInt
                val gridID : Int = latGridID * gridNum + lonGridID
                val temMinLon : Float = (lon - lonDiff).toFloat
                var temMaxLon : Float = (lon + lonDiff).toFloat
                val temMinLat : Float = (lat - latDiff).toFloat
                var temMaxLat : Float = (lat + latDiff).toFloat
                var minLonGridID : Int = floor((temMinLon - minLon) / lonGridLength).toInt
                var maxLonGridID : Int = floor((temMaxLon - minLon) / lonGridLength).toInt
                var minLatGridID : Int = floor((temMinLat - minLat) / latGridLength).toInt
                var maxLatGridID : Int = floor((temMaxLat - minLat) / latGridLength).toInt
                if (minLonGridID < 0){
                    minLonGridID = 0
                }
                if (maxLonGridID >= gridNum){
                    maxLonGridID = gridNum - 1
                }
                if (minLatGridID < 0){
                    minLatGridID = 0
                }
                if (maxLatGridID >= gridNum){
                    maxLatGridID = gridNum - 1
                }
                for (j <- minLonGridID to maxLonGridID) {
                    for (k <- minLatGridID to maxLatGridID) {
                        val temGridID : Int = k * gridNum + j
                        val timePos : Int = i._2
                        if (spi(temGridID)(timePos) == null) {
                            var temArrayBuffer : ArrayBuffer[(Int, Int, Float, Float)] = new ArrayBuffer[(Int, Int, Float, Float)]()
                            temArrayBuffer += i
                            spi(temGridID)(timePos) = temArrayBuffer
                        } else {
                            spi(temGridID)(timePos) += i
                        }
                    }
                }
                val time2 : Long = System.currentTimeMillis
                var D : ArrayBuffer[(Int, Int, Float, Float)] = spi(gridID)(i._2)
                val time3 : Long = System.currentTimeMillis
                if (D != null) {
                    var testID : Int = i._1
                    var testTime : Int = i._2
                    for (j <- (D.length - 1) to 0 by -1) {
                        if (ifLocateSafeArea(D(j), i, delta)) {
                            if (testID != D(j)._1) {
                                

                                if (testID < D(j)._1) {
                                    JP += ((testID, (testTime, D(j)._1)))
                                    
                                
                                } else {
                                    JP += ((D(j)._1, (testTime, testID)))
                                
                                }
                            }
                            
                        }
                    }
                }

                val time4 : Long = System.currentTimeMillis
                total_time1 += ((time2 - time1) / 1000.0).toDouble
                total_time2 += ((time3 - time2) / 1000.0).toDouble
                total_time3 += ((time4 - time3) / 1000.0).toDouble
            }
        }

        writer.write(total_time1 + "\n")
        writer.write(total_time2 + "\n")
        writer.write(total_time3 + "\n")
        var myLargeMBRminLon : Float = (currMinLon - lonDiff).toFloat
        var myLargeMBRmaxLon : Float = (currMaxLon + lonDiff).toFloat
        var myLargeMBRminLat : Float = (currMinLat - latDiff).toFloat
        var myLargeMBRmaxLat : Float = (currMaxLat + latDiff).toFloat

        
        pos = 0
        for (i <- 0 until myBaseSettings.spacePartitionsNum) {
            
            if (i != spaceID) {
                var indexPath : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + "day" + (dayID).toString + "/day" + (dayID).toString + "_index_3min/time" + timeID.toString + "space" + i.toString + ".txt"
                var par1 : (MBR, Array[(MBR, InvertedIndex)]) = readIndexFromFilePath(indexPath)
                var temPartition : (Int, Int, Int, MBR, Array[(MBR, InvertedIndex)]) = (dayID, spaceID, i, par1._1, par1._2)
                if (temPartition._4.ifIntersectMBR(new MBR(myLargeMBRminLon, myLargeMBRmaxLon, myLargeMBRminLat, myLargeMBRmaxLat))) {
                    val time1 : Long = System.currentTimeMillis
                    var readPath : String = myBaseSettings.rootPath.substring(5, myBaseSettings.rootPath.length) + "day" + (dayID).toString + "/day" + (dayID).toString + "_trajic_3min/par_ori" + timeID.toString + "zorder" + i.toString + ".tstjs"
                    var f : File = new File(readPath)
                    var bos : ByteArrayOutputStream = new ByteArrayOutputStream(f.length().toInt)
                    var in : BufferedInputStream = new BufferedInputStream(new FileInputStream(f))
                    var bufferSize : Int = 1024
                    var buffer : Array[Byte] = new Array[Byte](bufferSize)
                    var len : Int = 0
                    len = in.read(buffer, 0, bufferSize)
                    while(len != -1) {
                        bos.write(buffer, 0, len)
                        len = in.read(buffer, 0, bufferSize)
                    }
                    var zipData : Array[Byte] = bos.toByteArray()
                    var record : Array[(Int, Int, Float, Float)] = CompressMethod.unTrajicTotalFile(zipData, dayID, timeID, i, lookupTable, myBaseSettings)
                    val time2 : Long = System.currentTimeMillis
                    for (eachRecord <- record) {
                        pos += 1
                        val lon : Float = eachRecord._3
                        val lat : Float = eachRecord._4
                        if (lon >= myLargeMBRminLon &&
                            lon <= myLargeMBRmaxLon &&
                            lat >= myLargeMBRminLat &&
                            lat <= myLargeMBRmaxLat) {
                            
                            var lonGridID : Int = floor((lon - minLon) / lonGridLength).toInt
                            var latGridID : Int = floor((lat - minLat) / latGridLength).toInt
                            val gridID : Int = latGridID * gridNum + lonGridID
                            val temMinLon : Float = (lon - lonDiff).toFloat
                            var temMaxLon : Float = (lon + lonDiff).toFloat
                            val temMinLat : Float = (lat - latDiff).toFloat
                            var temMaxLat : Float = (lat + latDiff).toFloat
                            var minLonGridID : Int = floor((temMinLon - minLon) / lonGridLength).toInt
                            var maxLonGridID : Int = floor((temMaxLon - minLon) / lonGridLength).toInt
                            var minLatGridID : Int = floor((temMinLat - minLat) / latGridLength).toInt
                            var maxLatGridID : Int = floor((temMaxLat - minLat) / latGridLength).toInt
                            if (minLonGridID < 0){
                                minLonGridID = 0
                            }
                            if (maxLonGridID >= gridNum){
                                maxLonGridID = gridNum - 1
                            }
                            if (minLatGridID < 0){
                                minLatGridID = 0
                            }
                            if (maxLatGridID >= gridNum){
                                maxLatGridID = gridNum - 1
                            }
                            for (j <- minLonGridID to maxLonGridID) {
                                for (k <- minLatGridID to maxLatGridID) {
                                    val temGridID : Int = k * gridNum + j
                                    val timePos : Int = eachRecord._2
                                    if (spi(temGridID)(timePos) == null) {
                                        var temArrayBuffer : ArrayBuffer[(Int, Int, Float, Float)] = new ArrayBuffer[(Int, Int, Float, Float)]()
                                        temArrayBuffer += eachRecord
                                        spi(temGridID)(timePos) = temArrayBuffer
                                    } else {
                                        spi(temGridID)(timePos) += eachRecord
                                    }
                                }
                            }
                            
                            var D : ArrayBuffer[(Int, Int, Float, Float)] = spi(gridID)(eachRecord._2)
                            if (D != null) {
                                var testID : Int = eachRecord._1
                                var testTime : Int = eachRecord._2
                                for (j <- (D.length - 1) to 0 by -1) {
                                    if (ifLocateSafeArea(D(j), eachRecord, delta)) {
                                        if (testID != D(j)._1) {
                                            

                                            if (testID < D(j)._1) {
                                                JP += ((testID, (testTime, D(j)._1)))
                                    
                                                
                                            } else {
                                                JP += ((D(j)._1, (testTime, testID)))
                                
                                            }
                                        }
                            
                                    }
                                }
                            }
                        }
                    }
                    val time3 : Long = System.currentTimeMillis

                    total_time4 += ((time2 - time1) / 1000.0).toDouble
                    total_time5 += ((time3 - time2) / 1000.0).toDouble

                }
            }
        }

        writer.write(total_time4 + "\n")
        writer.write(total_time5 + "\n")
        writer.write(pos + "\n")
        writer.close()


        return JP
    }


    def trjPlaneSweepIndex(D : ArrayBuffer[(Int, Int, Float, Float)], delta : Double,
                            JP : ArrayBuffer[(Int, (Int, Int))], iter : (Int, Int, Float, Float)) : ArrayBuffer[(Int, (Int, Int))] = {
        if (D == null) {
            return JP
        }

        var loop = new Breaks
        loop.breakable{
        var testID : Int = iter._1
        var testTime : Int = iter._2
        for (j <- (D.length - 1) to 0 by -1) {
            if (D(j)._2 == testTime) {
                if (ifLocateSafeArea(D(j), iter, delta)) {
                    if (testID < D(j)._1) {
                        JP += ((testID, (testTime, D(j)._1)))
                    } else {
                        JP += ((D(j)._1, (testTime, testID)))
                    }
                }
            } else {
                assert(false)
            }
        }
        }
        return JP

    }

    def refine(result2 : Iterator[(Int, (Int, Int))], contiSnap : Int) : Iterator[(Int, Int)] = {
        var cTime : Map[Int, Int] = Map[Int, Int]()
        var sTime : Map[Int, Int] = Map[Int, Int]()
        var eTime : Map[Int, Int] = Map[Int, Int]()
        var matchList : ArrayBuffer[Int] = new ArrayBuffer[Int]()
        var result1 : Array[(Int, (Int, Int))] = result2.toArray.toSet.toArray
        var result = result1.sortBy(l => (l._2._1, l._2._2))
        var testID : Int = 0
        for (i <- 0 until result.length) {
            testID = result(i)._1
            var temTime : Int = result(i)._2._1
            var temID : Int = result(i)._2._2
            if (!cTime.contains(temID)) {
                cTime += temID -> 1
                sTime += temID -> temTime
            } else {
                if (!matchList.contains(temID)) {
                    if (temTime == (sTime.get(temID).get + cTime.get(temID).get)) {
                        cTime += temID -> (cTime.get(temID).get + 1)
                        if ((cTime.get(temID).get) == contiSnap) {
                            eTime += temID -> temTime
                            matchList += temID
                        }
                    } else {
                        cTime += temID -> 1
                        sTime += temID -> temTime
                    }
                }
            }
        }
        var finalMatchList : Array[(Int, Int)] = new Array[(Int, Int)](matchList.length)
        for (i <- 0 until matchList.length) {
            finalMatchList(i) = ((testID, matchList(i)))
        }
        return finalMatchList.iterator
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



}