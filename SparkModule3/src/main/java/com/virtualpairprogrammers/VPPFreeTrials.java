package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class VPPFreeTrials {
	
	public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {
		@Override
		public String call(String country) throws Exception {
			List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
			List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});
			
			if (topCountries.contains(country)) return country; 
			if (europeanCountries .contains(country)) return "EUROPE";
			else return "OTHER";
		}
	};

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("VPP Free trials").master("local[*]").getOrCreate();
//		Registering the UDF
		spark.udf().register("countryGrouping", countryGrouping, DataTypes.StringType);
		Dataset<Row> csvData = spark.read()
									.option("header", true)
									.option("inferSchema", true)
									.csv("src/main/resources/vppFreeTrials.csv");
		csvData = csvData.withColumn("country", callUDF("countryGrouping", col("country")))
						 .withColumn("label", when(col("payments_made").geq(1),lit(1)).otherwise(lit(0)));
//		Another way of writing the same thing as instead of the 'when' function
//		spark.udf().register("changeToZeroAndOneScale", (Integer number) -> {
//			if(number >= 1)
//				return 1;
//			else
//				return 0;
//		}, DataTypes.IntegerType);
//		csvData = csvData.withColumn("label", callUDF("changeToZeroAndOneScale", col("payments_made")));	
//		csvData.show();
		
		StringIndexer countryIndexer = new StringIndexer(); 
		csvData = countryIndexer.setInputCol("country")
					  .setOutputCol("countryIndex")
					  .fit(csvData)
					  .transform(csvData);
//		csvData.show();
		
//		Dataset<Row> countryIndexes = csvData.select("countryIndex").distinct();
//		IndexToString indexToString = new IndexToString();
//		indexToString.setInputCol("countryIndex").setOutputCol("value").transform(countryIndexes).show();

//		Below is the Production Standard way of writing the above 3 lines of code.
		new IndexToString()
			.setInputCol("countryIndex")
			.setOutputCol("value")
			.transform(csvData.select("countryIndex").distinct())
			.show();
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] {"countryIndex", "rebill_period", "chapter_access_count", "seconds_watched"});
		vectorAssembler.setOutputCol("features");
		Dataset<Row> inputData = vectorAssembler.transform(csvData).select("label", "features");
		inputData.show();
		
		Dataset<Row>[] trainingAndHoldOutData = inputData.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> trainingData = trainingAndHoldOutData[0];
		Dataset<Row> holdOutData = trainingAndHoldOutData[1];
		
//		DecisionTreeRegressor dtRegressor = new DecisionTreeRegressor();
//		dtRegressor.setMaxDepth(3); //Choose 3 to keep the model simple as our dataset is too small.
//		DecisionTreeRegressionModel model = dtRegressor.fit(trainingData);
//		model.transform(holdOutData).show();
//		The actual Decision Tree Model will look something like as below.
//		System.out.println(model.toDebugString());
		
//		Switched to Decision Tree Classification Model due to inaccurate results from Decision Tree Regression
		DecisionTreeClassifier dtClassifier = new DecisionTreeClassifier();
		dtClassifier.setMaxDepth(3);
		DecisionTreeClassificationModel model = dtClassifier.fit(trainingData);
		Dataset<Row> predictions = model.transform(holdOutData);
		predictions.show();
		System.out.println(model.toDebugString());
		
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		System.out.println("The accuracy of the model is " + evaluator.evaluate(predictions));
		
		RandomForestClassifier rfClassifier = new RandomForestClassifier();
		rfClassifier.setMaxDepth(3);
		RandomForestClassificationModel rfModel = rfClassifier.fit(trainingData);
		Dataset<Row> predictions2 = rfModel.transform(holdOutData);
		predictions2.show();
		
		System.out.println(rfModel.toDebugString());
		System.out.println("The accuracy of the forest model is " + evaluator.evaluate(predictions2));
		
		spark.close();
	}//end main

}//end class
