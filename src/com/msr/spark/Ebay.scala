package com.msr.spark

import org.apache.spark.sql.SQLContext

object Ebay {
  
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkContext
   Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

   val (zkQuorum, group, topics, numThreads) = ("localhost:2181","kelly","trainee","2")
  val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
  println("connected")
        
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(2))
  
  case class ebay(  auctionid:String,
                    bid:String,
                    bidtime:String,
                    bidder:String,
                    bidderrate:String,
                    openbid:String,
                    price:String,
                    item:String,
                    daystolive:String)
  
  def main(args: Array[String]): Unit = {

       val hctx: SQLContext = new HiveContext(sc)
    hctx.setConf("hive.metastore.uris", "thrift://10.1.7.49:9083")
    hctx.sql("use default")
    import hctx.implicits._
       
        ssc.checkpoint("checkpoint")
        println("topic connected")
        
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
        
        lines.foreachRDD{rdd => 
          
             //val header = (rdd.map(x => x.split("\r\n")).first())(0)
             
             //val rows = sc.parallelize((rdd.map(x => x.split("\r\n")).first().filter(row => row!=header )))
             
             val rows = sc.parallelize((rdd.map(x => x.split("\n")).first()))
             
             println(rows.count+" Number of rows fetched from kafka topic")
             
             val df = rows.map(line => line.split(",")).map(c => ebay(c(0),c(1),c(2),c(3),c(4),c(5),c(6),c(7),c(8)))            
            
             val table = df.toDF
             if(table.count > 0 ) 
             table.show
         
              table.write.mode(SaveMode.Append).saveAsTable("ebay")
        }
  
        ssc.start
        ssc.awaitTermination()
  }
}