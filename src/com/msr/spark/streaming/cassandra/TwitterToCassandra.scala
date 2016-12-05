package com.msr.spark.streaming.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import com.datastax.spark.connector.{ SomeColumns, _ }
import com.datastax.spark.connector.cql.CassandraConnector

object TwitterToCassandra {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  case class Tweets(tweet_id: Long, tweet: String, created_at: String)
  val Columns = SomeColumns("tweet_id", "tweet", "created_at")

  val filters = Array("India")

  System.setProperty("twitter4j.oauth.consumerKey", "FlRx3d0n8duIQ0UvGeGtTA")
  System.setProperty("twitter4j.oauth.consumerSecret", "DS7TTbxhmQ7oCUlDntpQQRqQllFFOiyNoOMEDD0lA")
  System.setProperty("twitter4j.oauth.accessToken", "1643982224-xTfNpLrARoWKxRh9KtFqc7aoB8KAAHkCcfC5vDk")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "PqkbuBqF3AVskgx1OKgXKOZzV7EMWRmRG0p8hvLQYKs")
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local[2]")
      .set("spark.cassandra.connection.host", "10.1.7.49")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    CassandraConnector(sparkConf).withSessionDo {
      session =>
        session.execute("create table if not exists msrc.tweets" +
          "(tweet_id bigint primary key,tweet text,created_at text)")
    }

    val stream = TwitterUtils.createStream(ssc, None, filters)

    stream.foreachRDD { rdd =>

      val comments = rdd.map(line => Tweets(line.getId, line.getText, line.getCreatedAt.toString))

      comments.take(5).foreach(println)

      comments.saveToCassandra("msrc", "tweets", Columns)
    }

    ssc.start
    ssc.awaitTermination()
  }

}