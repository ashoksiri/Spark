package com.msr.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.sql.SQLContext


object SQlContextExample1 {
  
  case class Person(id:Int,name:String)
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
   val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
    val sc = new SparkContext(sparkConf)
    def main(args: Array[String]): Unit = {
    
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    /*val source = sc.textFile("file:/home/user/work/dataset/test.csv")
    
    val data = source.map(line => line.split(",")).map(col => Person(col(0).toInt,col(1)))
    
    val table = data.toDF
    
    
    table.write.format("parquet").save("file:/home/user/work/dataset/parquet")
    println("Success")*/
    
    val partbl = sqlContext.read.parquet("file:/home/user/work/dataset/parquet")
    partbl.show
    
  }
  
}