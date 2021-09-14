package src.main.scala.util

import scala.math._

/**
* Some public functions
*/
object PublicFunc{

    val EARTH_RADIUS : Double = 6378.137

    private def haverSin(theta : Double) : Double = {
        val v = sin(theta / 2)
        v * v
    }

    private def convertDegreeToRadian(degree : Double) = degree * Pi / 180

    private def convertRadianToDegree(radian : Double) = radian * 180 / Pi

    /**
    * Calculate the real distance between two points
    * New method
    */
    def realDistance2(lon1 : Double, lat1 : Double, lon2 : Double, lat2 : Double) : Double = {
        val lonRadian1 : Double = convertDegreeToRadian(lon1)
        val latRadian1 : Double = convertDegreeToRadian(lat1)
        val lonRadian2 : Double = convertDegreeToRadian(lon2)
        val latRadian2 : Double = convertDegreeToRadian(lat2)
        val lonDiff    : Double = abs(lonRadian1 - lonRadian2)
        val latDiff    : Double = abs(latRadian1 - latRadian2)
        val height     : Double = haverSin(latDiff) + cos(latRadian1) * cos(latRadian2) * haverSin(lonDiff)
        val distance   : Double = 2 * EARTH_RADIUS * asin(sqrt(height))
        distance * 1000
    }

    /**
    * Calculate the real distance between two points
    * Old method, and there are some calculation errors
    */
    def realDistance(lonDiff : Double, latDiff : Double, latitude : Double) : Double = {
        var lonChangeRatio : Double = cos(latitude)
        var lonDistance    : Double = lonDiff * 111111 * lonChangeRatio
        var latDistance    : Double = latDiff * 111111
        sqrt((lonDistance * lonDistance + latDistance * latDistance))
    }

    /**
    * Test if a certain point is in a safe area
    */
    def ifLocateSafeArea(minLon : Double, maxLon : Double, minLat : Double,
                            maxLat : Double, lon : Double, lat : Double) : Boolean = {
        ((lon <= maxLon) && (lon >= minLon) && (lat <= maxLat) && (lat >= minLat))
    }

    /**
    * Change time to snapshot
    */
    def changeTimeToSnap(time : String, snapInterval : Int) : Int = {
        var hour        : Int = time.substring(0, 2).toInt
        var minute      : Int = time.substring(3, 5).toInt
        var second      : Int = time.substring(6, 8).toInt
        var totalSecond : Int = hour * 3600 + minute * 60 + second
        totalSecond / snapInterval
    }

    /**
    * Test if two MBR intersect
    */
    def ifIntersectOfMBR(cenLon1 : Double, cenLat1 : Double, widthLon1 : Double, heightLat1 : Double, 
                            cenLon2 : Double, cenLat2 : Double, widthLon2 : Double, heightLat2 : Double) : Boolean = {
        if ((widthLon1 / 2 + widthLon2 / 2) >= abs(cenLon1 - cenLon2) && (heightLat1 / 2 + heightLat2 / 2) >= abs(cenLat1 - cenLat2)){
            true
        }else{
            false
        }
    }

    /**
    * Calculate the ID of a partition
    */
    def getPartitionID(time : String, numParts : Int) : Int = {
        var hour        : Int    = time.substring(0, 2).toInt
        var minute      : Int    = time.substring(3, 5).toInt
        var second      : Int    = time.substring(6, 8).toInt
        var totalMinute : Int    = hour * 60 + minute
        var paredTime   : Double = ceil((23 * 60 + 59) / numParts)
        (totalMinute / paredTime).toInt
    }

}