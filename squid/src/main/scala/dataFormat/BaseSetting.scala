package src.main.scala.dataFormat

class BaseSetting extends Serializable{
    
    var delta : Double = -1.0
    
    var contiSnap : Int = -1
    
    var totalSnap : Int = -1
    
    var timeInterval : Int = -1

    var rootPath : String = null

    var daysNum : Int = -1

    var timePartitionsNum : Int = -1
    
    var spacePartitionsNum : Int = -1

    var totalTrajNums : Int = -1

    var patIDListSize : Int = -1
    
    var patIDList : Array[Int] = null

    var beginSecond : Int = -1

    var recordLength : Int = -1

    var indexSnapInterval : Int = -1
    
    var oneParSnapNum : Int = -1
    
    var lonGridNum : Int = -1
    
    var latGridNum : Int = -1
    
    var MINLON : Float = (-1.0).toFloat
    
    var MINLAT : Float = (-1.0).toFloat
    
    var MAXLON : Float = (-1.0).toFloat
    
    var MAXLAT : Float = (-1.0).toFloat
    
    var lonGridLength : Float = (-1.0).toFloat
    
    var latGridLength : Float = (-1.0).toFloat
    
    var trajNumEachSpace : Int = -1

    var dataset : String = ""

    var writeIndexToFileFlag : Int = -1

    var readIndexFromWhere : String = ""

    def setDelta(delta : Double){
        this.delta = delta
    }

    def setContiSnap(contiSnap : Int){
        this.contiSnap = contiSnap
    }

    def setTotalSnap(totalSnap : Int){
        this.totalSnap = totalSnap
    }

    def setTimeInterval(timeInterval : Int){
        this.timeInterval = timeInterval
    }

    def setRootPath(rootPath : String){
        this.rootPath = rootPath
    }

    def setDaysNum(daysNum : Int) {
        this.daysNum = daysNum
    }

    def setTimePartitionsNum(timePartitionsNum : Int){
        this.timePartitionsNum = timePartitionsNum
    }

    def setSpacePartitionsNum(spacePartitionsNum : Int){
        this.spacePartitionsNum = spacePartitionsNum
    }

    def setTotalTrajNums(totalTrajNums : Int){
        this.totalTrajNums = totalTrajNums
    }

    def setPatIDList(listSize : Int){
        this.patIDListSize = listSize
        this.patIDList = new Array[Int](listSize)
                            .zipWithIndex
                            .map(_._2)
    }

    def setBeginSecond(beginSecond : Int){
        this.beginSecond = beginSecond
    }

    def setRecordLength(recordLength : Int){
        this.recordLength = recordLength
    }

    def setIndexSnapInterval(indexSnapInterval : Int){
        this.indexSnapInterval = indexSnapInterval
    }

    def setOneParSnapNum(oneParSnapNum : Int){
        this.oneParSnapNum = oneParSnapNum
    }

    def setLonGridNum(lonGridNum : Int){
        this.lonGridNum = lonGridNum
    }

    def setLatGridNum(latGridNum : Int){
        this.latGridNum = latGridNum
    }

    def setMINLON(MINLON : Float){
        this.MINLON = MINLON
    }

    def setMINLAT(MINLAT : Float){
        this.MINLAT = MINLAT
    }

    def setMAXLON(MAXLON : Float){
        this.MAXLON = MAXLON
    }

    def setMAXLAT(MAXLAT : Float){
        this.MAXLAT = MAXLAT
    }

    def setLonGridLength(lonGridLength : Float){
        this.lonGridLength = lonGridLength
    }

    def setLatGridLength(latGridLength : Float){
        this.latGridLength = latGridLength
    }

    def setTrajNumEachSpace(trajNumEachSpace : Int){
        this.trajNumEachSpace = trajNumEachSpace
    }

    def setDataset(dataset : String){
        this.dataset = dataset
    }

    def setWriteIndexToFileFlag(writeIndexToFileFlag : Int){
        this.writeIndexToFileFlag = writeIndexToFileFlag
    }

    def setReadIndexFromWhere(readIndexFromWhere : String){
        this.readIndexFromWhere = readIndexFromWhere
    }

}