package com.msr.spark

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object WordCount {
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
    val sc = new SparkContext(sparkConf)
    
    val source = sc.textFile("file:/home/user/hadooptools/spark-1.6.0-bin-hadoop2.6/README.md")
   
    val mappeddata = source.filter(_.contains("spark"))
                           .flatMap(line => line.split(" "))
                           .map(x => (x,1)).reduceByKey(_+_)
    mappeddata.persist(StorageLevel.DISK_ONLY).foreach(println)
    
    println(source.count)
  }
}