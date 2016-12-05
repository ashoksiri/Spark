package com.msr.spark.sql.hbase

import org.apache.phoenix.spark._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object ReadHbase {
 
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  case class employees(emp_no:String,birth_date:String,first_name:String,last_name:String,gender:String,hire_date:String)
  
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  
  
  def main(args: Array[String]): Unit = {
    
    val rdd = sc.phoenixTableAsRDD("EMPLOYEES", Seq("EMP_NO","BIRTH_DATE","FIRST_NAME","LAST_NAME","GENDER","HIRE_DATE"), zkUrl=Some("localhost"))
    
    
    val emp = rdd.map(f => employees(f.get("EMP_NO") match{ case Some(i) => i.toString ; case None => ""},
                                    f.get("BIRTH_DATE") match{ case Some(i) => i.toString ; case None => ""},
                                    f.get("FIRST_NAME") match{ case Some(i) => i.toString ; case None => ""},
                                    f.get("LAST_NAME") match{ case Some(i) => i.toString ; case None => ""},
                                    f.get("GENDER") match{ case Some(i) => i.toString ; case None => ""},
                                    f.get("HIRE_DATE") match{ case Some(i) => i.toString ; case None => ""})).toDF
                                    
    

    
         emp.show                           
    //val rdd1 = rdd.map(f=> f.values).flatMap { x => x.toSeq }
    
    //rdd1.take(5).foreach(println)
  }
}