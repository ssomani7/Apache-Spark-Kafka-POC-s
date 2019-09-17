package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.List;

public class CourseRecommendationCollaborativeFiltering {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
//		Creating a Spark Session Object
		SparkSession spark = SparkSession.builder().appName("Gym Competitors").master("local[*]").getOrCreate();
//		Reading the csv file, inferSchema helps in casting the string datatype to int or other datatypes inplace.
		Dataset<Row> csvData = spark.read()
									.option("header", true)
									.option("inferSchema", true)
									.csv("src/main/resources/VPPcourseViews.csv");
		csvData = csvData.withColumn("proportionWatched", col("proportionWatched").multiply(100));
		
//		csvData.groupBy("userId").pivot("courseId").sum("proportionWatched").show();
//		Dataset<Row>[] trainingAndHoldoutData = csvData.randomSplit(new double[] {0.9, 0.1});
//		Dataset<Row> trainingData = trainingAndHoldoutData[0];
//		Dataset<Row> holdoutData = trainingAndHoldoutData[1];
		
		ALS als = new ALS()
						  .setMaxIter(10)
						  .setRegParam(0.1)
						  .setUserCol("userId")
						  .setItemCol("courseId")
						  .setRatingCol("proportionWatched");
		
//		ALSModel model = als.fit(trainingData);
//		Dataset<Row> predictions = model.transform(holdoutData);
		ALSModel model = als.fit(csvData);
		
		Dataset<Row> userRecs = model.recommendForAllUsers(5);
//		userRecs.show();
		List<Row> userRecsList = userRecs.takeAsList(5);
		
		for(Row r : userRecsList) {
			int userId = r.getAs(0);
			String recs = r.getAs(1).toString();
			System.out.println("User " + userId + " might want to recommend " + recs);
			System.out.println("This user has already watched: ");
			csvData.filter("userID = " + userId).show();
		}
		spark.close();
	}//end main

}//end class
