package com.msr.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.datastax.spark.connector.SomeColumns
import java.io.PrintWriter
import java.io.File
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import java.io.IOException
import scala.tools.nsc.interpreter.IMain
import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.io.AbstractFile

object TestCassandraLoading extends App{
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  val url :String = "jdbc:mysql://10.1.7.49/employees"
  val prop = new java.util.Properties();
  val table :String = "employees"
  val classLocation:String = "/home/scala"
  val dynaPackage :String = "com.example"
  
  prop.put("driver","com.mysql.jdbc.Driver")
  prop.put("username","root")
  prop.put("password","hadoop")
  
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  
  val emp = sql.read.jdbc(url,table,prop);
  
  val columns = emp.dtypes.map(c => c._1+":"+(c._2 match {  case "IntegerType" =>  "Int"
                                                          case "DateType" => "java.sql.Date"
                                                          case "StringType" => "String"
                                                          })).mkString(",")
  
  println(columns)
  
 
  //emp.dtypes.map(a => a._1+"-->"+a._2).foreach(println )
    generateClass(table, columns)
   /* val classLoader = compileFiles(fileLocation.getAbsolutePath)
    val empClass = classLoader.findClass(dynaPackage + "." + getClassName(table))
    val ctor = empClass.getDeclaredConstructors()(0)
    val instance = ctor.newInstance("1","2016-04-01","abc","xyz","M","2016-01-15");
    println("Instantiated class: " + instance.getClass.getCanonicalName)
    println(instance.toString)*/
    
   
  def writeCaseClass(file: File, className: String, header: String): Unit = {
  val writer: PrintWriter = new PrintWriter(file)
  writer.println("package " + dynaPackage)
  writer.println("case class " + className + "(")
  writer.println(header)
  writer.println(") {}")
  writer.flush()
  writer.close()
  println("Done")
  }
  
  @throws[IOException]
def compileFiles(classDir: String): AbstractFileClassLoader = {
  val files = recursiveListFiles(new File(classDir))
                  .filter(_.getName.endsWith("scala"))
  println("Loaded files: \n" + files.mkString("[", ",\n", "]"))

  val settings: GenericRunnerSettings = new GenericRunnerSettings(err => println("Interpretor error: " + err))
  settings.usejavacp.value = true
  val interpreter: IMain = new IMain(settings)
  files.foreach(f => {
    interpreter.compileSources(new BatchSourceFile(AbstractFile.getFile(f)))
  })

  interpreter.getInterpreterClassLoader()
}
  
  def recursiveListFiles(dir: File): Array[File] = {
  val these = dir.listFiles
  these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }
  
  def getClassName(filename: String): String = filename.split("\\.")(0)
  
  
  @throws[IOException]
private def generateClass(filename: String, header: String) {
  val className: String = getClassName(filename)
  val fileDir: String = classLocation + File.separator + dynaPackage.replace('.', File.separatorChar)
  println(fileDir)
  new File(fileDir).mkdirs
  println(new File(fileDir).exists)
  //val classFile: String = fileDir + File.separator + className + ".scala"
  //val file: File = new File(classFile)

  //writeCaseClass(file, className, header)
}
}