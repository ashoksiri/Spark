package com.msr.test

import java.io.File
import scala.reflect.io.Path
import java.io.PrintWriter
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.interpreter.IMain
import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.io.AbstractFile


object FileTest {

  def main(args: Array[String]): Unit = {
  
    import reflect.runtime.currentMirror
    import tools.reflect.ToolBox
    val toolbox = currentMirror.mkToolBox()
    import toolbox.u._
    import scala.io.Source
  
    val className: String = "employees"
    val headers: String = "emp_no:Int,birth_date:java.sql.Date,first_name:String,last_name:String,gender:String,hire_date:java.sql.Date"
    val classLocation: String = "/tmp/scala"
    val packageName: String = "com.example"

    val classFullPath = classLocation + "/" + packageName.replace('.', '/')
    println(classFullPath)
    val file: Path = Path(classFullPath)
    file.createDirectory(failIfExists = false);

    val writer: PrintWriter = new PrintWriter(new File(classFullPath + "/" + className + ".scala"))
    writer.println("package " + packageName)
    writer.println("case class " + className + "(")
    writer.println(headers)
    writer.println(")")
    writer.flush()
    writer.close()

    val files = recursiveListFiles(new File(classLocation))
    
    files.foreach { x => 
   
      if(x.isFile()&&x.toString().endsWith(".scala")) {
        val fileContents = Source.fromFile(x).getLines.mkString("\n")
        println(fileContents)
        val tree = toolbox.parse("import com.msr.test._; "+fileContents)
        val compiledCode = toolbox.compile(tree)

        val emp = compiledCode().getClass().getConstructor()
        val empObject = emp.newInstance("1", "2016-05-01", "as", "sd", "M", "2016-04-05")

        println(empObject)
      }

      
    }
    
    
  }
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
}