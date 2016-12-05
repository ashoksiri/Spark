package com.msr.spark.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage._
import org.apache.log4j._

object NetWorkWordCount {
  
  //Logger.getLogger("org").setLevel(Level.OFF)
  //Logger.getLogger("akka").setLevel(Level.OFF)
  
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc,Seconds(2))
  
  //ssc.checkpoint("hdfs://localhost:9000/spark")
  
  def main(args: Array[String]): Unit = {
  
    val lines = ssc.socketTextStream("localhost", 4455.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    lines.foreachRDD{ rdd => 
      val words = rdd.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
      wordCounts.foreach(println)
    }
     
    ssc.start
    ssc.awaitTermination()
  }
}