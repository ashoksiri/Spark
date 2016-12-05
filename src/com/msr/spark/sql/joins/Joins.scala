package com.msr.spark.sql.joins

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.spark.joins.EmployeesDBSchemas
import java.sql.Date
import java.text.SimpleDateFormat

object Joins {
  
   Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  
  def todate(s:String):Date=new Date(new SimpleDateFormat("yyyy-MM-dd").parse(s).getTime)
  
  
  def main(args: Array[String]): Unit = {
    
     
    
    val file1 = "file:/home/user/Documents/dataset/employees_db/employees.csv"
    val file2 = "file:/home/user/Documents/dataset/employees_db/salaries.csv"
    val file3 = "file:/home/user/Documents/dataset/employees_db/titles.csv"
    val file4 = "file:/home/user/Documents/dataset/employees_db/dept_emp.csv"
    val file5 = "file:/home/user/Documents/dataset/employees_db/dept_manager.csv"
    val file6 = "file:/home/user/Documents/dataset/employees_db/departments.csv"
    
    val employees = sc.textFile(file1).map(_.split(",")).map(c => Row(c(0).toInt,todate(c(1)),c(2),c(3),c(4),todate(c(5))))
    val salaries = sc.textFile(file2).map(_.split(",")).map(c => Row(c(0).toInt,c(1).toInt,todate(c(2)),todate(c(3))))
    val titles = sc.textFile(file3).map(_.split(",")).map(c => Row(c(0).toInt,c(1),todate(c(2)),todate(c(3))))
    val dept_emp = sc.textFile(file4).map(_.split(",")).map(c => Row(c(0).toInt,c(1),todate(c(2)),todate(c(3))))
    val dept_manager = sc.textFile(file5).map(_.split(",")).map(c => Row(c(0),c(1).toInt,todate(c(2)),todate(c(3))))
    val departments = sc.textFile(file6).map(_.split(",")).map(c => Row(c(0),c(1)))
    
    val empdf = sqlContext.createDataFrame(employees, EmployeesDBSchemas.employees)
    val saldf = sqlContext.createDataFrame(salaries, EmployeesDBSchemas.salaries)
    val ttldf = sqlContext.createDataFrame(titles, EmployeesDBSchemas.titles)
    val deptempdf = sqlContext.createDataFrame(dept_emp, EmployeesDBSchemas.dept_emp)
    val deptmandf = sqlContext.createDataFrame(dept_manager, EmployeesDBSchemas.dept_manager)
    val deptdf = sqlContext.createDataFrame(departments, EmployeesDBSchemas.departments)
    
    
    empdf.join(saldf,empdf("emp_no")===saldf("emp_no"))
    .select(empdf("emp_no"),concat(empdf("first_name"),lit(" "),empdf("last_name")),saldf("salary"))
    .where(saldf("salary") > 70000).distinct().show
    
    empdf.join(ttldf,empdf("emp_no")===ttldf("emp_no"),"left")
    .select(empdf("emp_no"),concat(empdf("first_name"),lit(" "),empdf("last_name")),ttldf("title")).show
   
    deptempdf.groupBy(deptempdf("dept_no")).count.show
    
    //select a.emp_no as manager ,a.dept_no as department ,count as employees from dept_manager a join 
    //(select dept_no,count(emp_no) as count from dept_emp group by dept_no) as x on a.dept_no = x.dept_no ;
    
    deptmandf.as("a").join(deptempdf.groupBy(deptempdf("dept_no")).count.as("x"))
    .filter("a.dept_no = x.dept_no").select("a.emp_no","a.dept_no","x.count").show
    
    // empdf.show
    //saldf.show
   // ttldf.show
   // deptempdf.show
   // deptmandf.show
    //deptdf.show
    
  }
}