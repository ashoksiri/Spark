package com.msr.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds

object FlumeStreaming {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val stream = FlumeUtils.createPollingStream(ssc, "localhost", 6666, StorageLevel.MEMORY_ONLY_SER_2)
    stream.foreachRDD {
      x =>

        val data = x.map(line => line.event)

        var rdd = data.map(x => new String(x.getBody.array()))

        val mytable = rdd.flatMap(line =>line.split(" "))
          .map(c => (c,1)).reduceByKey(_+_)
            
          mytable.foreach(println)
    }

    ssc.start
    ssc.awaitTermination
  }
}