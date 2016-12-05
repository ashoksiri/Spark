package com.msr.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object ReadingJson {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]): Unit = {
  
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
                              .set("spark.eventLog.dir","file:/home/user/sparkhistory_logs")
                              
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val file = "file:/home/user/Documents/dataset/employees.json"
    
     val empdf = sqlContext.read.json(file)
    
    empdf.show
    
  }
}