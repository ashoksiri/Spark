package com.msr.spark.streaming

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{ Logger, Level }
import twitter4j.HashtagEntity
import twitter4j.MediaEntity
import twitter4j.UserMentionEntity
import twitter4j.Status
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

object TwitterStreaming {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val filters = Array("India")

  System.setProperty("twitter4j.oauth.consumerKey", "FlRx3d0n8duIQ0UvGeGtTA")
  System.setProperty("twitter4j.oauth.consumerSecret", "DS7TTbxhmQ7oCUlDntpQQRqQllFFOiyNoOMEDD0lA")
  System.setProperty("twitter4j.oauth.accessToken", "1643982224-xTfNpLrARoWKxRh9KtFqc7aoB8KAAHkCcfC5vDk")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "PqkbuBqF3AVskgx1OKgXKOZzV7EMWRmRG0p8hvLQYKs")
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val stream = TwitterUtils.createStream(ssc, None, filters)

    stream.foreachRDD { rdd =>

      val comments = rdd.map(line => line.getText)//.flatMap(_.split(" ")).map(c => (c,1)).reduceByKey(_+_)

      if (comments.count > 0) {
        comments.foreach(println)
      }
    }

    ssc.start
    ssc.awaitTermination()
  }
}