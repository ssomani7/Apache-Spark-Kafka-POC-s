package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//import org.apache.spark.sql.functions;
//Below static import is done so as to directly call the methods without using any prefixes of the class name.
//It's just a style of coding, no performance improvements. More readable code.
import static org.apache.spark.sql.functions.*;
import java.util.List;
import java.util.Locale;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class SparkSQL {
	
	public static void main(String[] args) {
		//To handle the session warnings
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		//Boilerplate code to start the Spark SQL session
		SparkSession spark = SparkSession.builder().appName("testingSparkSQL").master("local[*]").getOrCreate();		
//		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
//		dataset.show();
		
//		long numberOfRecords = dataset.count();
//		System.out.println("There are " + numberOfRecords + " records.");	
//		Row firstRow = dataset.first();
//		String str1 = firstRow.get(2).toString();
//		System.out.println("'get' method operates on coulmns as indexes starting from zero.\nThe third index is " + str1);
//		
//		String str2 = firstRow.getAs("subject").toString();
//		System.out.println("\n'getAs' method operates directly on the column names from the header section.\nResult is = " + str2);
//		
//		int year = Integer.parseInt(firstRow.getAs("year"));
//		System.out.println("\nAlways cast the objects explicitly from the 'getAs' method.\nResult is = " + year);
		
//		To the 'filter' method parameters are what you typically write in the sql WHERE clauses.
//		Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art'");
//		Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
//		modernArtResults.show();
//		Below is another way of writing the same thing as above
//		Dataset<Row> modernArtResults2 = dataset.filter(row -> row.getAs("subject").equals("Modern Art") &&
//														Integer.parseInt(row.getAs("year")) >= 2007);
//		modernArtResults2.show();
		
//		Filter using columns
//		Column subjectColumn = dataset.col("subject");
//		You can also do above step by
//		Column subjectColumn = functions.col("subject");
//		Column yearColumn = dataset.col("year");
//		Column yearColumn = functions.col("year");
//		Dataset<Row> modernArtResults3 = dataset.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
//		modernArtResults3.show();
		
//		Using the static imports we can skip the variable declarations.
//		Dataset<Row> modernArtResults4 = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
//		modernArtResults4.show();
		
//		Using SparkSQL Temporary View we can write proper SQL syntax for querying and filtering results
//		dataset.createOrReplaceTempView("any_table_name_here");
//		Dataset<Row> results = spark.sql("select score, year from any_table_name_here where subject = 'French'");
//		Dataset<Row> results = spark.sql("select avg(score) from any_table_name_here where subject = 'French'");
//		results.show();
		
//		In Memory Data can be used for testing same as JUnit test cases.
//		List<Row> inMemory = new ArrayList<Row>();
//		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
//		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
//		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
//		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
//		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		
//		Each entry is a column from the table we want to create. 
//		StructField[] fields = new StructField[] {
//			new StructField("level", DataTypes.StringType, false, Metadata.empty()),
//			new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
//		};
//		
//		StructType schema = new StructType(fields);
//		Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/viewing figures/biglog.txt");
//		dataset.createOrReplaceTempView("logging_table");
//		Dataset<Row> results = spark.sql("select level, count(datetime) from logging_table group by level order by level");
//		Dataset<Row> results = spark.sql("select level, date_format(datetime, 'yyyy') as Month, count(1) as total from logging_table group by level, Month");
//		Dataset<Row> results = spark.sql
//				("select level, date_format(datetime, 'MMMM') as Month, count(1) as total "
//						+ "from logging_table group by level, Month "
//						+ "order by cast(first(date_format(datetime, 'M')) as int), level");
//		results.show(100);
		
//		Using the Dataframe API, prefer this method.
//		dataset = dataset.selectExpr("level","date_format(datetime, 'MMMM') as Month");
		
//		We can do the same thing using "functions" class (everything is treated as columns) as follows
//		dataset = dataset.select(col("level"), 
//								 date_format(col("datetime"), "MMMM").alias("Month"),
//								 date_format(col("datetime"), "M").alias("MonthNumber").cast(DataTypes.IntegerType));
//		Basic Rule = Whenever you do group by on a column, you need to do an Aggregate operation on the non-grouping column
//		dataset = dataset.groupBy(col("level"), col("Month"), col("MonthNumber")).count();
//		dataset = dataset.orderBy(col("MonthNumber"), col("level"));
//		This is just for showcasing the drop method.
//		dataset = dataset.drop(col("MonthNumber"));
		
//		Pivot Table - Use when have 2 groupings and aggregation.
//		Object[] monthsInYear = new Object[] {
//				"January", "February", "March",
//				"April", "May", "June", "July",
//				"August", "September", "October",
//				"November", "December"
//		};
//		List<Object> monthList = Arrays.asList(monthsInYear);
//		dataset = dataset.groupBy("level").pivot("Month", monthList).count();
//		dataset.show(100);
		
//		Using a UDF in Spark SQL
		dataset.createOrReplaceTempView("logging_table");
		spark.udf().register("monthsInNumbers", (String monthName) -> {
			if(monthName.equals("January"))
				return 1;
			if(monthName.equals("February"))
				return 2;
			if(monthName.equals("March"))
				return 3;
			if(monthName.equals("April"))
				return 4;
			if(monthName.equals("May"))
				return 5;
			if(monthName.equals("June"))
				return 6;
			if(monthName.equals("July"))
				return 7;
			if(monthName.equals("August"))
				return 8;
			if(monthName.equals("September"))
				return 9;
			if(monthName.equals("October"))
				return 10;
			if(monthName.equals("November"))
				return 11;
			return 12;
		}, DataTypes.IntegerType);
		
		Dataset<Row> results = spark.sql
		("select level, date_format(datetime, 'MMMM') as Month, count(1) as total "
				+ "from logging_table group by level, Month order by monthsInNumbers(Month), level" );
		results.show();
		spark.close();
	}//end main

}//end class
