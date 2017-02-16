package com.msr.spark.poc

import java.io.BufferedReader
import java.io.FileReader
import java.io.File
import java.text.SimpleDateFormat

object Test {
  
  def main(args: Array[String]): Unit = {
    
     val splits = """\[(.*?)\] \[(.*?)\] (.*)""".r;
     
     val source = "[Sun Jun 05 03:40:02 2016] [notice] Digest: generating secret for digest authentication ..."
     
    val format = new SimpleDateFormat("E MMM mm HH:mm:ss yyyy")
     val reader = new BufferedReader(new FileReader(new File("/home/user/Documents/httpderror.log")))
     
     var a = "";
     
     a=reader.readLine
      var splits(identity:String ,report_type:String ,description:String )= a
      
      println(new java.sql.Date(format.parse(identity).getTime)+"--- "+report_type+" ---"+description)
     
  }
}