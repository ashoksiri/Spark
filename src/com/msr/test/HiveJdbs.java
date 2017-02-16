package com.msr.test;

import java.sql.DriverManager;
import org.apache.hive.jdbc.HiveDriver;
public class HiveJdbs {

	public static void main(String args[]) throws Exception{

		Class.forName("org.apache.hive.jdbc.HiveDriver");
		
		System.out.println(DriverManager.getConnection("jdbc:hive2://localhost:10000/default","root","hadoop"));
		
	}

}
