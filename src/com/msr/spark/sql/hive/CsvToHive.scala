package com.msr.spark.sql.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.hive.HiveContext
import scala.reflect.runtime.universe

object CsvToHive {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  case class employees(emp_no: Long, birth_date: String, first_name: String, last_name: String, gender: String, hire_date: String)
  def todate(s: String): Date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse(s).getTime)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
      .set("spark.eventLog.dir", "file:/home/user/sparkhistory_logs")

    val sc = new SparkContext(conf)
    val hctx:SQLContext = new HiveContext(sc)
    hctx.setConf("hive.metastore.uris","thrift://10.1.7.49:9083")
    hctx.sql("use default")
    
    import hctx.implicits._

    val file1 = "file:/home/user/Documents/dataset/employees_db/employees.csv"
    val employee = sc.textFile(file1).map(_.split(",")).map(c => employees(c(0).toInt, c(1), c(2), c(3), c(4), c(5)))
    val df = employee.toDF

    hctx.sql("show tables").show
    df.show

  }
}