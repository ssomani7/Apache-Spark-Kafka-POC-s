package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeansSummary;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitorsClustering {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
//		Creating a Spark Session Object
		SparkSession spark = SparkSession.builder().appName("Gym Competitors K-Means Clustering").master("local[*]").getOrCreate();
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
//		csvData.show();
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		Dataset<Row> inputData = vectorAssembler.setInputCols(new String [] {"GenderVector", "Age", "Height", "Weight", "NoOfReps"})
					   							.setOutputCol("features")
					   							.transform(csvData)
					   							.select("features");
		inputData.show();
		
		KMeans kMeans = new KMeans();
		
		for(int noOfClusters = 2; noOfClusters <= 8; noOfClusters++) {
			kMeans.setK(noOfClusters);
			System.out.println("Cluster Number = " + noOfClusters);
			
			KMeansModel model = kMeans.fit(inputData);
			Dataset<Row> predictions = model.transform(inputData);
			predictions.show();
			
//			Vector[] clusterCenters = model.clusterCenters();
//			for(Vector v : clusterCenters)
//				System.out.println(v);
			
			predictions.groupBy("prediction").count().show();
			System.out.println("Sum of Squared Erros is " + model.computeCost(inputData));
//			KMeansSummary kSummary = model.summary();
			ClusteringEvaluator evaluator = new ClusteringEvaluator();
			System.out.println("The Silhouette with Squared Euclidean Distance is " + evaluator.evaluate(predictions));
		}
//		kMeans.setK(3);
//		
//		KMeansModel model = kMeans.fit(inputData);
//		Dataset<Row> predictions = model.transform(inputData);
//		predictions.show();
//		
//		Vector[] clusterCenters = model.clusterCenters();
//		for(Vector v : clusterCenters)
//			System.out.println(v);
//		
//		predictions.groupBy("prediction").count().show();
//		System.out.println("Sum of Squared Erros is " + model.computeCost(inputData));
////		KMeansSummary kSummary = model.summary();
//		ClusteringEvaluator evaluator = new ClusteringEvaluator();
//		System.out.println("The Silhouette with Squared Euclidean Distance is " + evaluator.evaluate(predictions));
		
		spark.close();
	}//end main

}//end class
