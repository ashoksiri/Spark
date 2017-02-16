package com.msr.test

import scala.tools.nsc.interpreter.AbstractFileClassLoader
import java.io.PrintWriter
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.GenericRunnerSettings
import java.io.IOException
import java.io.File
import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.io.AbstractFile

object Demo {
  /**
  * Location to store temporary scala source files with generated case classes
  */
val classLocation: String = "/tmp/dynacode"

/**
  * Package name to store the case classes
  */
val dynaPackage: String = "com.example.dynacsv"

/**
  * Construct this data based on your data model e.g. see data type for Person and Address below.
  * Notice the format of header, it can be substituted directly in a case class definition.
  */
val personCsv: String = "PersonData.csv"
val personHeader: String = "title: String, firstName: String, lastName: String, age: Int, height: Int, gender: Int"

val addressCsv: String = "AddressData.csv"
val addressHeader: String = "street1: String, street2: String, city: String, state: String, zipcode: String"

/**
  * Utility method to extract class name from CSV file
  * @param filename CSV file
  * @return Class name extracted from csv file name stripping out ".ext"
  */
def getClassName(filename: String): String = filename.split("\\.")(0)

/**
  * Generate a case class and persist to file
  * @param file External file to write to
  * @param className Class name
  * @param header case class parameters
  */
def writeCaseClass(file: File, className: String, header: String): Unit = {
  val writer: PrintWriter = new PrintWriter(file)
  writer.println("package " + dynaPackage)
  writer.println("case class " + className + "(")
  writer.println(header)
  writer.println(") {}")
  writer.flush()
  writer.close()
}

/**
  * Generate case class and write to file
  * @param filename CSV File name (should be named ClassName.csv)
  * @param header Case class parameters. Format is comma separated: name: DataType
  * @throws IOException if there is problem writing the file
  */
@throws[IOException]
private def generateClass(filename: String, header: String) {
  val className: String = getClassName(filename)
  val fileDir: String = classLocation + File.separator + dynaPackage.replace('.', File.separatorChar)
  new File(fileDir).mkdirs
  val classFile: String = fileDir + File.separator + className + ".scala"
  val file: File = new File(classFile)

  writeCaseClass(file, className, header)
}

/**
  * Helper method to search code files in directory
  * @param dir Directory to search in
  * @return
  */
def recursiveListFiles(dir: File): Array[File] = {
  val these = dir.listFiles
  these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
}

/**
  * Compile scala files and keep them loaded in memory
  * @param classDir Directory storing the generated scala files
  * @throws IOException if there is problem reading the source files
  * @return Classloader that contains the compiled external classes
  */
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

//Test Address class
def testAddress(classLoader: AbstractFileClassLoader) = {
  val addressClass = classLoader.findClass(dynaPackage + "." + getClassName(addressCsv))
  val ctor = addressClass.getDeclaredConstructors()(0)
  val instance = ctor.newInstance("123 abc str", "apt 1", "Hello world", "HW", "12345")
  println("Instantiated class: " + instance.getClass.getCanonicalName)
  println(instance.toString)
}

//Test person class
def testPerson(classLoader: AbstractFileClassLoader) = {
  val personClass = classLoader.findClass(dynaPackage + "." + getClassName(personCsv))
  val ctor = personClass.getDeclaredConstructors()(0)
  val instance = ctor.newInstance("Mr", "John", "Doe", 25: java.lang.Integer, 165: java.lang.Integer, 1: java.lang.Integer)
  println("Instantiated class: " + instance.getClass.getCanonicalName)
  println(instance.toString)
}

//Test generated classes
def testClasses(classLoader: AbstractFileClassLoader) = {
  testAddress(classLoader)
  testPerson(classLoader)
}

//Main method
def main(args: Array[String]) {
  try {
    generateClass(personCsv, personHeader)
    generateClass(addressCsv, addressHeader)
    val classLoader = compileFiles(classLocation)
    testClasses(classLoader)
  }
  catch {
    case e: Exception => e.printStackTrace()
  }
}
}