package com.msr.spark.poc

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import java.text.SimpleDateFormat

object ErrorLogProcessing {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    .set("spark.eventLog.dir", "file:/home/user/sparkhistory_logs")

  val sc = new SparkContext(conf)
  val sqlContext: SQLContext = new SQLContext(sc)

  //sqlContext.setConf("hive.metastore.uris", "thrift://localhost:9083")
  //sqlContext.sql("use default")

  case class httpderrorLog(identity: String, report_type: String, description: String)

  import sqlContext.implicits._

  val splits = """\[(.*?)\] \[(.*?)\] (.*)""".r;

  def main(args: Array[String]): Unit = {

    val source = sc.textFile("file:/home/user/Documents/httpderror.log")

    println(source.count)

    val errordf = source.map(makeColumns).map(c => httpderrorLog(c(0),c(1),c(2))).toDF
    
    errordf.show
    //errordf.write.mode(SaveMode.Append).saveAsTable("errorlog")
    
  }

  def makeColumns(line: String): Array[String] = {
  val format = new java.text.SimpleDateFormat("E MMM mm HH:mm:ss yyyy")
    var splits(identity, report_type, description) = line
    Array(new java.sql.Date(format.parse(identity).getTime).toString, report_type, description)
  }
}
