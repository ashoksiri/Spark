package com.msr.spark.sql.jdbc


import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType


object MySqlJdbc {
  
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)
  
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val sqlContext:SQLContext = new SQLContext(sc)
  
   
  def main(args: Array[String]): Unit = {
   
     val prop = new java.util.Properties
     prop.put("user","root")
     prop.put("password","hadoop")
     prop.put("driverClass","com.mysql.jdbc.Driver")
     
     val uri = "jdbc:mysql://10.1.7.49:3306/employees"
     val table = "employees"
     
     val dept_manager = sqlContext.read.jdbc(uri,table,prop)
     
     dept_manager.withColumn("birth_date", dept_manager("birth_date").cast(StringType)).withColumn("hire_date", dept_manager("hire_date").cast(StringType)).repartition(1).limit(100).write.format("com.databricks.spark.xml").option("rootTag","employees").option("rowTag","employee").mode(SaveMode.Overwrite).save("file:/home/user/Documents/dataset/orc/employees")
     println("success")
     
   } 
}