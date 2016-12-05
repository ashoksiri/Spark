package com.spark.joins

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DateType

/**
 * @author user
 */
object EmployeesDBSchemas {
  
  val ebay = StructType(Array(
        StructField("auction_id", IntegerType, true),
        StructField("bid", DateType, true),
        StructField("bit_time", StringType, true),
        StructField("bidrate",StringType, true),
        StructField("openid", StringType, true),
        StructField("price", DateType, true) ))
        
 
  
  val employees = StructType(Array(
   
        StructField("emp_no", IntegerType, true),
        StructField("birth_date", DateType, true),
        StructField("first_name", StringType, true),
        StructField("last_name",StringType, true),
        StructField("gender", StringType, true),
        StructField("hire_date", DateType, true) ))
        
        
 
  val departments = StructType(Array(
   
        StructField("dept_no", StringType, true),
        StructField("dept_name", StringType, true)))
        
     val dept_manager = StructType(Array(
   
        StructField("dept_no", StringType, true),
        StructField("emp_no", IntegerType, true),
        StructField("from_date", DateType, true),
        StructField("to_date",DateType, true)))
      
      val dept_emp = StructType(Array(
   
        StructField("emp_no", IntegerType, true),
        StructField("dept_no", StringType, true),
        StructField("from_date",DateType, true),
        StructField("to_date",DateType, true)))  
        
     val emp_image = StructType(Array(
   
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("emp_image", StringType, true)))    
          
      val salaries = StructType(Array(
   
        StructField("emp_no", IntegerType, true),
        StructField("salary", IntegerType, true),
        StructField("from_date", DateType, true),
        StructField("to_date",DateType, true)))  
        
        val titles = StructType(Array(
   
        StructField("emp_no", IntegerType, true),
        StructField("title", StringType, true),
        StructField("from_date", DateType, true),
        StructField("to_date",DateType, true)))
        
       
}                               
