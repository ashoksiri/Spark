package com.msr.spark.sql.hbase

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import java.sql.DriverManager
import org.apache.phoenix.jdbc.PhoenixDriver
import org.apache.phoenix.spark._

object CsvToHbase {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      val con = DriverManager.getConnection("jdbc:phoenix:10.1.7.49")
      println(con)
      val rs = con.createStatement().execute("create table if not exists employees("
        + "emp_no bigint primary key"
        + ",birth_date varchar(200),first_name varchar(200) ,"
        + "last_name varchar(200),gender varchar(200),hire_date varchar(200))")
    } catch {
      case e: Exception => println(e.getMessage)
    }
    
    val csv = "file:/home/user/Documents/dataset/employees_db/employees.csv"

    var table = sc.textFile(csv).map(line => line.replace("\"", ""))
      .map(line => line.split(","))
      .map(columns => (columns(0).toLong, columns(1), columns(2), columns(3), columns(4), columns(5)))

    table.saveToPhoenix("employees", Seq("EMP_NO", "BIRTH_DATE", "FIRST_NAME", "LAST_NAME", "GENDER", "HIRE_DATE"), zkUrl = Some("localhost"))
    println("success")

  }
}