package com.msr.spark

import org.apache.spark._

object SparkContextExample {
  
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster(args(0)).setAppName(this.getClass.getName)
    val sc = new SparkContext(sparkConf)
    
    println(sc +"--------- this is SparkContext Object")
  }
}