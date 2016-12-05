package com.msr.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object KafkaStreaming {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val (zkQuorum, group, topics, numThreads) = ("localhost:2181", "kelly", "trainee", "2")
  val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(2))

  def main(args: Array[String]): Unit = {

    println("topic connected")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    lines.foreachRDD { rdd => rdd.flatMap(_.split(" ")).map(c => (c, 1)).reduceByKey(_ + _).foreach(println) }
    ssc.start
    ssc.awaitTermination
  }
}