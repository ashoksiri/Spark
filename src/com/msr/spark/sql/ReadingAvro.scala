package com.msr.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.databricks.spark.avro._

object ReadingAvro {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]): Unit = {
  
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
                              .set("spark.eventLog.dir","file:/home/user/sparkhistory_logs")
                              
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val file = "file:/home/user/Documents/dataset/employees.avro"
    
    val empdf = sqlContext.read.avro(file)
    
    empdf.show
  }
}