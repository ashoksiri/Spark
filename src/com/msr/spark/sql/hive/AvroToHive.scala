package com.msr.spark.sql.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import com.databricks.spark.avro._

object AvroToHive {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
      .set("spark.eventLog.dir", "file:/home/user/sparkhistory_logs")

    val sc = new SparkContext(conf)
    val hctx: SQLContext = new HiveContext(sc)
    hctx.setConf("hive.metastore.uris", "thrift://10.1.7.49:9083")
    hctx.sql("use default")

    import hctx.implicits._

    val file = "file:/home/user/Documents/dataset/employees.avro"
    
    val empdf = hctx.read.avro(file)
    
    empdf.write.format("orc").saveAsTable("employees")

    println("Success")
  }
}