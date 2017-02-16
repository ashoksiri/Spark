package com.msr.spark.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    
    if(args.length<5)
    {
      println("""Example Command line Arguments --class com.msr.spark.wordcount.WordCount master deploymode executor_instances input output""")
      println("""spark-submit --class com.msr.spark.wordcount.WordCount 
              /home/user/workspace/Spark/target/FirstSpark-0.0.1-SNAPSHOT.jar 
              local client 2 hdfs://localhost:9000/user/sample.txt hdfs://localhost:9000/user/wordcount/spark""");
      
      return
    }
    
    
    val conf = new SparkConf().setMaster(args(0))
    .setAppName(this.getClass.getName)
    .set("spark.submit.deployMode",args(1)).set("spark.executor.instances",args(2))
  
    val sc = new SparkContext(conf)
    
   // val source = sc.textFile("file:/home/user/hadooptools/spark-1.6.0-bin-hadoop2.6/README.md")
    val source = sc.textFile(args(3))
    
    val wordsmap = source.filter { x => x.contains("spark") }.flatMap(l => l.split(" ").toList).map(x => (x,1))
    
    val wordcounts = wordsmap.reduceByKey((a,b) => a+b )
    
    wordcounts.saveAsTextFile(args(4))
  }
}