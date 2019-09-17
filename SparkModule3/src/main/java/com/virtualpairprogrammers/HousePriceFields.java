package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceFields {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("House Price Fields").master("local[*]").getOrCreate();
		Dataset<Row> csvData = spark.read()
									.option("header", true)
									.option("inferSchema", true)
									.csv("src/main/resources/kc_house_data.csv");
//		csvData.printSchema();
//		csvData.show();
//		csvData.describe().show(); 
		csvData = csvData.drop("id", "date", "waterfront", "view", "condition", "grade", "yr_renovated", "zipcode", "lat", "long");
//		for(String col : csvData.columns()) {
//			System.out.println("The correlation between the price and " + col + " is " + csvData.stat().corr("price", col));
//		}
//		System.out.println("The correlation between the price and sqft_living is " + csvData.stat().corr("price", "sqft_living")); 
		csvData = csvData.drop("sqft_lot", "sqft_lot15", "sqft_living15", "yr_built");
		for(String col1 : csvData.columns()){
			for(String col2 : csvData.columns()) {
				System.out.println("The correlation between the " + col1 + " and " + col2 + " is " + csvData.stat().corr(col1, col2));
			}
		}
		spark.close();
	}//end main

}//end class
