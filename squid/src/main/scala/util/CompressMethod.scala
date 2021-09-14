package src.main.scala.util

import scala.math._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext

import java.nio.ByteBuffer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import java.util.ArrayList

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import src.main.scala.dataFormat.BaseSetting
import src.main.scala.util.trajic.Trajic
import src.main.scala.util.trajic.GPSPoint



object CompressMethod {

    def unGzip(data : Array[Byte]) : Array[Byte] = {
        var bis  : ByteArrayInputStream = new ByteArrayInputStream(data)
        var gzip : GZIPInputStream = new GZIPInputStream(bis)
        var buf  : Array[Byte] = new Array[Byte](1024)
        var baos : ByteArrayOutputStream = new ByteArrayOutputStream()
        var num : Int = -1
        num = gzip.read(buf, 0, buf.length)
        while(num != -1){
        baos.write(buf, 0, num)
        num = gzip.read(buf, 0, buf.length)
        }
        val b = baos.toByteArray()
        baos.flush()
        baos.close()
        gzip.close()
        bis.close()
        b
        
    }

    def unZip(data : Array[Byte]) : Array[Byte] = {
        var bis : ByteArrayInputStream = new ByteArrayInputStream(data)
        var zip : ZipInputStream = new ZipInputStream(bis)
        var b : Array[Byte] = null
        while(zip.getNextEntry() != null) {
            var buf : Array[Byte] = new Array[Byte](1024)
            var baos : ByteArrayOutputStream = new ByteArrayOutputStream()
            var num : Int = -1
            num = zip.read(buf, 0, buf.length)
            while((num != -1)) {
                baos.write(buf, 0, num)
                num = zip.read(buf, 0, buf.length)
            }
            b = baos.toByteArray
            baos.flush()
            baos.close()
        }
        zip.close()
        bis.close()
        return b
    }

    def unTrajic(data : Array[Byte]) : Array[Byte] = {
        val unTrajicData : Array[Byte] = Trajic.decompressByteArrayNoTime(data)
        unTrajicData
    }

    def unTrajicGetPoints(data : Array[Byte]) : Array[GPSPoint] = {
        val unTrajicPoints : Array[GPSPoint] = Trajic.decompressByteArrayNoTimeGetPoints(data).toArray(new Array[GPSPoint](0))
        unTrajicPoints
    }

    def unTrajicTotalFile(zipData : Array[Byte], dayID : Int, timeID : Int, spaceID : Int,
                            lookupTable : Array[Array[Int]], myBaseSettings : BaseSetting) : Array[(Int, Int, Float, Float)] = {
        
        
        var totalTrajNums = myBaseSettings.totalTrajNums
        var trajNumEachSpace = myBaseSettings.trajNumEachSpace
        var oneParSnapNum = myBaseSettings.oneParSnapNum
        var result : ArrayBuffer[(Int, Int, Float, Float)] = new ArrayBuffer[(Int, Int, Float, Float)]()

        var trajNumData : Array[Byte] = new Array[Byte](4)
        for (t <- 0 until 4) {
            trajNumData(t) = zipData(t)
        }
        val thisTrajNum : Int = ByteBuffer.wrap(trajNumData).getInt()
        
        

        val controlInfoLength : Int = thisTrajNum * 4
        val testData : Array[Byte] = new Array[Byte](controlInfoLength)
        for (t <- 0 until controlInfoLength){
            testData(t) = zipData(t + 4)
        }

        for (i <- 0 until thisTrajNum) {
            val trajNumPos : Int = i
            var helpArray : Array[Byte] = new Array[Byte](4)
            for (t <- 0 until 4) {
                helpArray(t) = testData(trajNumPos * 4 + t)
            }
            var startPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
            var  stopPos = 0
            if (i != (thisTrajNum - 1)) {
                for (t <- 0 until 4) {
                    helpArray(t) = testData((i + 1) * 4 + t)
                }
                stopPos = ByteBuffer.wrap(helpArray).getInt() + controlInfoLength
            } else {
                stopPos = zipData.length - 4
            }
            
            var helpArray2 : Array[Byte] = new Array[Byte](stopPos - startPos)
            for (t <- 0 until (stopPos - startPos)) {
                helpArray2(t) = zipData(t + startPos + 4)
            }
            var dataPoints : Array[GPSPoint] = CompressMethod.unTrajicGetPoints(helpArray2)

            var curr_id : Int = lookupTable(dayID)(timeID * totalTrajNums * 3 + spaceID * trajNumEachSpace * 3 + i * 3 + 2)
            for (j <- 0 until dataPoints.length) {
                var curr_time : Int = timeID * oneParSnapNum + j
                var point : GPSPoint = dataPoints(j)
                var lon : Float = point.lon
                var lat : Float = point.lat
                result += ((curr_id, curr_time, lon, lat))
            }

        }
        
        result.toArray
    }

}