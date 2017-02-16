package com.msr.test

import org.apache.spark.SparkContext
import org.apache.log4j._
object Testing {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext("local","Hai");
    
    val rdd = sc.textFile("file:/home/user/Documents/dataset/sfpd.csv")
    val rdd2 = rdd.filter(l => !l.contains("IncidntNum"))
    val s = rdd2.map(_.split("^\".*?\"$")).take(50).foreach(l => println(l.size))//.map(clean)//.map(line => line.split("`"))
        
    //s.take(50).foreach(x =>if(x.split("`").size==16) println(x) )
  
  }

  def clean(line: Array[String]): String = {
    var s = "";
    val itr = line.iterator
    while (itr.hasNext) {
      var l = itr.next();
      if (l.charAt(0) != '(')
        s = s + l.replace(",", "`")
      else
        s = s + l
    }
    s
  }
}