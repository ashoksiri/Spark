package com.msr.spark.sql.udf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StringType
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import java.text.SimpleDateFormat
import java.sql.Date
import org.apache.spark.sql.Row

object UdfExample {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SParkProgram")
      .set("spark.eventLog.dir", "file:/home/user/sparkhistory_logs")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
   

    val dataset = Seq((0, "hello"), (1, "world")).toDF("id", "text")

    dataset.registerTempTable("hello")

    val upper: String => String = _.toUpperCase

    import org.apache.spark.sql.functions.udf

    val upperUDF = udf(upper)

    dataset.select(upperUDF('text)).show

    sqlContext.udf.register("upper", upper)

    sqlContext.sql("select upper(text) from hello").show
  }
}