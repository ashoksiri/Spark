package com.msr.spark.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster(args(0)).setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    
    val source = sc.textFile("file:/home/user/hadooptools/spark-1.6.0-bin-hadoop2.6/README.md")
    
    val wordsmap = source.filter { x => x.contains("spark") }.flatMap(l => l.split(" ")).map(x => (x,1))
    
    val wordcounts = wordsmap.reduceByKey((a,b) => a+b )
    
    wordcounts.foreach(println)
  }
}