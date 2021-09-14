package src.main.scala.dataFormat

class MBR extends Serializable{

    var minLon : Float = _
    var maxLon : Float = _
    var minLat : Float = _
    var maxLat : Float = _

    def this(minLon : Float, maxLon : Float, minLat : Float, maxLat : Float) = {
        this()
        this.minLon = minLon
        this.maxLon = maxLon
        this.minLat = minLat
        this.maxLat = maxLat
    }

    def getArea() : Double = {
        1.0 * (this.maxLon - this.minLon) * (this.maxLat - this.minLat)
    }

    def getWidthLon() : Float = {
        this.maxLon - this.minLon
    }

    def getHeightLat() : Float = {
        this.maxLat - this.minLat
    }

    def getCenLon() : Float = {
        (this.maxLon + this.minLon) / 2
    }

    def getCenLat() : Float = {
        (this.maxLat + this.minLat) / 2
    }

    def ifIntersectMBR(other : MBR) : Boolean = {
        val widthLon1   : Float = other.getWidthLon
        val heightLat1  : Float = other.getHeightLat
        var cenLon1     : Float = other.getCenLon
        val cenLat1     : Float = other.getCenLat
        val widthLon2   : Float = this.getWidthLon
        val heightLat2  : Float = this.getHeightLat
        var cenLon2     : Float = this.getCenLon
        val cenLat2     : Float = this.getCenLat
        if ((widthLon1 / 2 + widthLon2 / 2) >= math.abs(cenLon1 - cenLon2) &&
            (heightLat1 / 2 + heightLat2 / 2) >= math.abs(cenLat1 - cenLat2)){
            true
        }else{
            false
        }
    }

}