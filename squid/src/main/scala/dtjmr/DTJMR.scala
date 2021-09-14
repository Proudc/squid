package src.main.scala.dtjmr

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Partitioner


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
import java.text.SimpleDateFormat;
import java.util.Date;


import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
import scala.collection.mutable.Set
import scala.collection.mutable.Map

import src.main.scala.selfPartitioner.keyOfPartitioner


object DTJMR {
    
    def main(args : Array[String]) : Unit = {
        val spark = SparkSession
                    .builder()
                    .config("spark.driver.maxResultSize", "20g")
                    .appName("DTJ-MR")
                    .getOrCreate()
        val sc = spark.sparkContext

        // "porto" or "singapore"
        var dataset : String = "porto"

        // Default parameter is 15000
        var totalTrajNum : Int = 15000

        // Default parameter is 150
        var delta : Double = 150.toDouble

        // Default parameter is 240 (20minutes)
        var contiSnap : Int = 240

        
        var daysNum : Int = 0

        var datasetList : Array[String] = Array("porto")
        var totalTrajNumList : Array[Int] = Array(10000, 10000, 12500, 15000, 17500, 20000)
        var deltaList : Array[Double] = Array(50, 50, 100, 150, 200, 250)
        var contiSnapList : Array[Int] = Array(120, 120, 180, 240, 300, 360)
        var daysNumList : Array[Int] = Array(0, 0, 2, 3)

        // for (currDataSet <- datasetList) {
        //     for (currTotalTrajNum <- totalTrajNumList) {
        //         entry(sc, currDataSet, currTotalTrajNum, delta, contiSnap, daysNum)
        //     }
        // }

        // for (currDataSet <- datasetList) {
        //     for (currDelta <- deltaList) {
        //         entry(sc, currDataSet, totalTrajNum, currDelta, contiSnap, daysNum)
        //     }
        // }

        // for (currDataSet <- datasetList) {
        //     for (currContiSnap <- contiSnapList) {
        //         entry(sc, currDataSet, totalTrajNum, delta, currContiSnap, daysNum)
        //     }
        // }

        // for (currDataSet <- datasetList) {
        //     for (currDaysnum <- daysNumList) {
        //         entry(sc, currDataSet, totalTrajNum, delta, contiSnap, currDaysnum)
        //     }
        // }


        entry(sc, dataset, totalTrajNum, delta, contiSnap, daysNum)
        entry(sc, dataset, totalTrajNum, delta, contiSnap, daysNum)
        entry(sc, dataset, totalTrajNum, delta, contiSnap, daysNum)

        
        

    }

    def entry(sc : SparkContext, dataset : String, totalTrajNum : Int, delta : Double, contiSnap : Int, daysNum : Int) : Unit = {
        // The root path of the data set
        var rootPath : String = "file:///mnt/data1/billion/"
        // The root path of the join result
        val queryResultRootWritePath : String = "/home/sigmod/code/python/sigmod_exper/dtjmr_" + dataset + "_"

        val gridNum : Int = 100

        var minLon : Float = 0.0.toFloat
        var maxLon : Float = 0.0.toFloat
        var minLat : Float = 0.0.toFloat
        var maxLat : Float = 0.0.toFloat


        var rootReadPathFromFile : String = ""

        if (daysNum == 1) {
            rootReadPathFromFile = rootPath + "/" + dataset.toString + "/" + totalTrajNum.toString + "/day0/day0_par_3min/par*.tstjs"
        } else {
            rootReadPathFromFile = rootPath + "/" + dataset.toString + "/" + totalTrajNum.toString + "/day_" + daysNum.toString + "/day0_par_3min/par*.tstjs"
        }


        if (dataset == "porto") {
            minLon = -8.77.toFloat
            maxLon = -8.23.toFloat
            minLat = 40.9.toFloat
            maxLat = 41.5.toFloat
        } else {
            minLon = 103.6.toFloat
            maxLon = 104.0.toFloat
            minLat = 1.22.toFloat
            maxLat = 1.58.toFloat
        }
        



        // preprocessing
        val indexRDD : RDD[(Int, Int, Float, Float)] = sc.binaryRecords(rootReadPathFromFile, 14)
                                                         .map(l => mapDataSingle(l))
                                                         .mapPartitions(sort)
                                                         .cache()
        val time1 : Long = System.currentTimeMillis
        // indexRDD.count()
        val time2 : Long = System.currentTimeMillis
        val indexTime : Double = ((time2 - time1) / 1000.0).toDouble

        val time3 : Long = System.currentTimeMillis
        var joinRDD : RDD[(Int, (Int, Int))] = indexRDD.mapPartitions(l => join(l, delta, minLon, maxLon, minLat, maxLat, gridNum))
                                                       .partitionBy(new keyOfPartitioner(totalTrajNum))
        val time4 : Long = System.currentTimeMillis
        val joinTime : Double = ((time4 - time3) / 1000.0).toDouble

        val time5 : Long = System.currentTimeMillis
        val tempA  : Array[(Int, Int)] = joinRDD.mapPartitions(l => refine(l, contiSnap))
                                                .collect()
        val time6 : Long = System.currentTimeMillis
        val refineTime : Double = ((time6 - time5) / 1000.0).toDouble
        println("\n----------------------------------------------------------")
        println("Set index time on the index: " + indexTime)
        println("Join time: " + joinTime)
        println("Refine time: " + refineTime)
        println("The number of close contacts is: " + tempA.length)
        println("----------------------------------------------------------")
        // val queryResultWritePath : String = queryResultRootWritePath + totalTrajNum.toString + "_" + delta.toInt.toString + "_" + contiSnap.toString + "_" + daysNum.toString + ".txt"
        // var writer : BufferedWriter = new BufferedWriter(new FileWriter(new File(queryResultWritePath)))
        // writer.write(indexTime.toString + "\t" + joinTime.toString + "\t" + refineTime.toString + "\t" + tempA.length.toString)
        // writer.close()

    }

    def preprocessFromFile(rootReadPath : String, sc : SparkContext) : RDD[(Int, Int, Float, Float)] = {
        val inputRDD : RDD[(Int, Int, Float, Float)] = sc.binaryRecords(rootReadPath, 14)
                                                         .map(mapDataSingle)
                                                         .persist(StorageLevel.MEMORY_AND_DISK)
        val time1 : Long = System.currentTimeMillis
        inputRDD.count()
        val time2 : Long = System.currentTimeMillis
        println("Set index time on the index: " + ((time2 - time1) / 1000.0).toDouble)
        inputRDD
    }

    def testCluster(l : Array[Byte]) : (Int, Int) = {
        var num1 : Int = ByteBuffer.wrap(l.slice(0, 4)).getInt
        var num2 : Int = ByteBuffer.wrap(l.slice(4, 6)).getShort.toInt
        var num3 : Float = ByteBuffer.wrap(l.slice(6, 10)).getFloat
        var num4 : Float = ByteBuffer.wrap(l.slice(10, 14)).getFloat
        var num5 : Int = num1
        var num6 : Int = num1 - num2
        var num7 : Float = num3 - num4
        var num8 : Float = num3 + num4
        (num5, num6)
    }

    def mapDataSingle(l : Array[Byte]) : (Int, Int, Float, Float) = {
        var num1 : Int = ByteBuffer.wrap(l.slice(0, 4)).getInt
        var num2 : Int = ByteBuffer.wrap(l.slice(4, 6)).getShort.toInt
        var num3 : Float = ByteBuffer.wrap(l.slice(6, 10)).getFloat
        var num4 : Float = ByteBuffer.wrap(l.slice(10, 14)).getFloat
        if (num2 < 0) {
            num2 = num2 + 32768
        }
        (num1, num2, num3, num4)
    }


    def sort(iter : Iterator[(Int, Int, Float, Float)]) : Iterator[(Int, Int, Float, Float)] = {
        var temList : Array[(Int, Int, Float, Float)] = iter.toArray
        temList = temList.sortBy(l => (l._2, l._1))
        temList.iterator

    }

    def timeChange(date : String, time : String) : Int = {
        val baseDate : String = "2015-04-01"
        val baseDateStringArray : Array[String] = baseDate.split("-")
        val baseDateIntArray : Array[Int] = Array(baseDateStringArray(0).toInt, baseDateStringArray(1).toInt, baseDateStringArray(2).toInt)
        val dateStringArray : Array[String] = date.split("-")
        val dateIntArray : Array[Int] = Array(dateStringArray(0).toInt, dateStringArray(1).toInt, dateStringArray(2).toInt)
        val timeStringArray : Array[String] = time.split(":")
        val timeIntArray : Array[Int] = Array(timeStringArray(0).toInt, timeStringArray(1).toInt, timeStringArray(2).toInt)
        val second1 : Int = (dateIntArray(0) - baseDateIntArray(0)) * 365 * 24 * 60 * 60
        val second2 : Int = (dateIntArray(1) - baseDateIntArray(1)) * 30 * 24 * 60 * 60
        val second3 : Int = (dateIntArray(2) - baseDateIntArray(2)) * 24 * 60 * 60
        val second4 : Int = timeIntArray(0) * 60 * 60 + timeIntArray(1) * 60 + timeIntArray(2)
        return (second1 + second2 + second3 + second4) / 5
    }


    def distSIndex(D1 : (Int, Int, Float, Float), D2 : (Int, Int, Float, Float)) : Double = {
        var dLon : Double = D1._3 - D2._3
        var dLat : Double = D1._4 - D2._4
        var disOneDegree : Int = 111111
        var lLon : Double = dLon * disOneDegree
        var lLat : Double = dLat * disOneDegree
        return (Math.sqrt(lLon * lLon + lLat * lLat))
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

    def join(iter : Iterator[(Int, Int, Float, Float)], delta : Double, minLon : Float,
                        maxLon : Float, minLat : Float, maxLat : Float, gridNum : Int) : Iterator[(Int, (Int, Int))] = {
        val partitionID : Int = TaskContext.get.partitionId
        var path = "/home/dtjmr-join-singapore-" + partitionID.toString + ".txt"
        var writer : BufferedWriter = new BufferedWriter(new FileWriter(new File(path)))
        

        val lonDiff : Double = delta / 111111.0
        val latDiff : Double = delta / 111111.0
        val lonGridLength : Float = (maxLon - minLon) / gridNum
        val latGridLength : Float = (maxLat - minLat) / gridNum

        var JP  : ArrayBuffer[(Int, (Int, Int))] = new ArrayBuffer[(Int, (Int, Int))](20000000)
        var spi : Array[Array[ArrayBuffer[(Int, Int, Float, Float)]]] = Array.ofDim[ArrayBuffer[(Int, Int, Float, Float)]](gridNum * gridNum, 34560)
        
        var pos = 0
        
        var total_time1 : Double = 0.0
        var total_time2 : Double = 0.0
        var total_time3 : Double = 0.0
        

        for (i <- iter) {
            pos += 1
            if (i._4 != -1) {

                val time1 : Long = System.currentTimeMillis
                val lon : Float = i._3
                val lat : Float = i._4
                var lonGridID : Int = floor((lon - minLon) / lonGridLength).toInt
                var latGridID : Int = floor((lat - minLat) / latGridLength).toInt
                if (lonGridID < 0){
                    lonGridID = 0
                }
                if (lonGridID >= gridNum){
                    lonGridID = gridNum - 1
                }
                if (latGridID < 0){
                    latGridID = 0
                }
                if (latGridID >= gridNum){
                    latGridID = gridNum - 1
                }

                val gridID : Int = latGridID * gridNum + lonGridID
                val temMinLon : Float = (lon - lonDiff).toFloat
                val temMaxLon : Float = (lon + lonDiff).toFloat
                val temMinLat : Float = (lat - latDiff).toFloat
                val temMaxLat : Float = (lat + latDiff).toFloat
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

                for (j <- minLonGridID to maxLonGridID){
                    for (k <- minLatGridID to maxLatGridID){
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
        writer.close


        return JP.iterator
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
        var result1 : Array[(Int, (Int, Int))] = result2.toArray
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

}
