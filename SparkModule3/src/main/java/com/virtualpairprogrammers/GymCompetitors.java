package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitors {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
//		Creating a Spark Session Object
		SparkSession spark = SparkSession.builder().appName("Gym Competitors").master("local[*]").getOrCreate();
//		Reading the csv file, inferSchema helps in casting the string datatype to int or other datatypes inplace.
		Dataset<Row> csvData = spark.read()
									.option("header", true)
									.option("inferSchema", true)
									.csv("src/main/resources/GymCompetition.csv");
		csvData.printSchema();
		StringIndexer genderIndexer = new StringIndexer();
		genderIndexer.setInputCol("Gender");
		genderIndexer.setOutputCol("GenderIndex");
		csvData = genderIndexer.fit(csvData).transform(csvData);
//		csvData.show();
		
		OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator();
		genderEncoder.setInputCols(new String[] {"GenderIndex"});
		genderEncoder.setOutputCols(new String[] {"GenderVector"});
		csvData = genderEncoder.fit(csvData).transform(csvData);
		csvData.show();
		
		VectorAssembler featureVector = new VectorAssembler();
		featureVector.setInputCols(new String[] {"Age", "Height", "Weight", "GenderVector"});
		featureVector.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = featureVector.transform(csvData);
		csvDataWithFeatures.show();
		
		Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
		modelInputData.show();
		
		LinearRegression linearRegressionHelperObject = new LinearRegression();
		LinearRegressionModel model = linearRegressionHelperObject.fit(modelInputData);
		System.out.println("The model has intercept = " + model.intercept() + " and the coefficient = " + model.coefficients());
		model.transform(modelInputData).show();
		spark.close();
	}//end main

}//end class
