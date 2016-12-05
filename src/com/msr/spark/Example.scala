package com.msr.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.DateType


object Example {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]): Unit = {
  
    val conf = new SparkConf().setMaster("local[*]").setAppName("SParkProgram")
                              .set("spark.eventLog.dir","file:/home/user/sparkhistory_logs")
                              
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println(sc)
    
    //val source = sc.textFile("file:/home/user/Documents/dataset/sfpd.csv")
    //source.take(5).foreach(println)

    
    val customers = sc.parallelize(List(("Alice", "2016-05-01", 50.00),
                                    ("Alice", "2016-05-03", 45.00),
                                    ("Alice", "2016-05-04", 55.00),
                                    ("Bob", "2016-05-01", 25.00),
                                    ("Bob", "2016-05-04", 29.00),
                                    ("Bob", "2016-05-06", 27.00)))
                                    .map{x => 
                                        
                                        var a:String = null
                                        var b:Date =  null
                                        var c:Double = 0
                                        val sf = new SimpleDateFormat("yyyy-MM-dd")
                                          a=x._1
                                          b = new Date(sf.parse(x._2).getTime)
                                          c = x._3
                                          (a,b,c)
                                         }.map(f => Row(f._1.to,f._2,f._3))
                                    
                                    //.map(changeDatatypes)
                                    //.map(f => Row(f._1.to,f._2,f._3))
         
          

           val schema =  StructType(Array(StructField("name",StringType,true),
                                          StructField("date",DateType,true),
                                          StructField("amountSpent",DoubleType,true)))                    
                               
            val df = sqlContext.createDataFrame(customers, schema)                              
                               df.show
    
  }
}