package com.msr.spark.streaming.hive

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.sql.SaveMode

object TwitterToHive {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  case class Tweets(tweet_id: Long, tweet: String, created_at: String)
  
  val filters = Array("India")
  
  System.setProperty("twitter4j.oauth.consumerKey", "FlRx3d0n8duIQ0UvGeGtTA")
  System.setProperty("twitter4j.oauth.consumerSecret", "DS7TTbxhmQ7oCUlDntpQQRqQllFFOiyNoOMEDD0lA")
  System.setProperty("twitter4j.oauth.accessToken", "1643982224-xTfNpLrARoWKxRh9KtFqc7aoB8KAAHkCcfC5vDk")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "PqkbuBqF3AVskgx1OKgXKOZzV7EMWRmRG0p8hvLQYKs")
  
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val hc = new HiveContext(ssc.sparkContext)
    import hc.implicits._
    
    hc.setConf("hive.metastore.uris","thrift://10.1.7.49:9083")
    hc.sql("use default")
    
     val prop = new java.util.Properties
     prop.put("user","root")
     prop.put("password","hadoop")
     prop.put("driverClass","com.mysql.jdbc.Driver")
     
     val uri = "jdbc:mysql://10.1.7.49:3306/test"
     val table = "vmstat"
    
    val stream = TwitterUtils.createStream(ssc, None, filters)

    stream.foreachRDD { rdd =>

      val comments = rdd.map(line => Tweets(line.getId, line.getText, line.getCreatedAt.toString)).toDF

      comments.take(5).foreach(println)
      
      comments.write.format("orc").mode(SaveMode.Append).saveAsTable("tweets")
    }
    ssc.start()
    ssc.awaitTermination()
  }
}