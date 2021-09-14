package src.main.scala.selfPartitioner

import org.apache.spark.Partitioner

class keyOfPartitioner(numParts : Int) extends Partitioner{
    override def numPartitions : Int = numParts
    override def getPartition(key : Any) : Int = {
        val keys : Int = key.asInstanceOf[Int]
        keys
    }
}