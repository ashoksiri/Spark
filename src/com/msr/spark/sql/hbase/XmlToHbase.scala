package com.msr.spark.sql.hbase

import java.sql.DriverManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.phoenix.spark._

object XmlToHbase {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  case class employees(emp_no: Long, birth_date: String, first_name: String, last_name: String, gender: String, hire_date: String)

  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    .setMaster("local")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

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

    val empdf = sqlContext.read.format("com.databricks.spark.xml")
      .option("rootTag", "employees")
      .option("rowTag", "employee")
      .load("file:/home/user/Documents/dataset/employees.xml")

    
      val temptable = empdf.map {

      case Row(birth_date, emp_no, first_name, last_name, gender, hire_date) =>

        employees(emp_no.asInstanceOf[Long], birth_date.toString, first_name.toString, last_name.toString, gender.toString, hire_date.toString())
    }

    temptable.saveToPhoenix("employees", Seq("EMP_NO", "BIRTH_DATE", "FIRST_NAME", "LAST_NAME", "GENDER", "HIRE_DATE"), zkUrl = Some("localhost"))
    println("success")

  }
}