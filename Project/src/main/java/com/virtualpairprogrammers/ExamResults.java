package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class ExamResults {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("SparkSQLAggregateFucntions").master("local[*]").getOrCreate();
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
//		dataset = dataset.groupBy("subject").max("score"); 
//		dataset = dataset.groupBy(col("subject")).agg(max(col("score")).cast(DataTypes.IntegerType).alias("Max Score"));
//		dataset = dataset.groupBy(col("subject")).pivot("year").agg(round(avg(col("score")), 2).alias("Average"),
//																	round(stddev(col("score")), 2).alias("Std Deviation"));

//		You can add a new column to the existing dataset using "withColumn"
//		dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
		
//		Using UDF (User Defined Functions) is a much more efficient way to manipulate exisiting dataset
//		spark.udf().register("hasPassed", (String grade) -> grade.equals("A+"), DataTypes.BooleanType);
		spark.udf().register("hasPassed", (String grade, String subject) -> {
			if(subject.equals("Biology"))
				if(grade.startsWith("A"))
					return true;
				else
					return false;
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		}, DataTypes.BooleanType);
		dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));
		dataset.show();
				
		spark.close();
	}//end main

}//end class
