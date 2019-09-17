package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class VPPChapterViews {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("VPP Chapter Views").master("local[*]").getOrCreate();
		Dataset<Row> csvData = spark.read()
									.option("header", true)
									.option("inferSchema", true)
									.csv("src/main/resources/vppChapterViews/*.csv");
//		By doing filter of "is cancelled = false", we are filtering out all the records where 'is_cancelled' is set to TRUE
//		That means, we are keeping the records where the course hasn't been cancelled, i.e, 'false'
//		So by applying this filter we are getting rid of all the rows where 'is_cancelled = true'.
		csvData = csvData.filter("is_cancelled = false");
//		csvData.show();
//		Once we are done with this filter, we can drop the 'is_cancelled' column as we no longer need it.
		csvData = csvData.drop("observation_date", "is_cancelled");
//		csvData.show();

//		Getting rid of NULL values
		csvData = csvData.withColumn("firstSub", when(col("firstSub").isNull(), 0).otherwise(col("firstSub")))
						 .withColumn("all_time_views", when(col("all_time_views").isNull(), 0).otherwise(col("all_time_views")))
						 .withColumn("last_month_views", when(col("last_month_views").isNull(), 0).otherwise(col("last_month_views")))
						 .withColumn("next_month_views", when(col("next_month_views").isNull(), 0).otherwise(col("next_month_views")));
//		csvData.show();

//		Renaming the column we want to predict as 'label'
		csvData = csvData.withColumnRenamed("next_month_views", "label");
//		Now let's encode the non-numeric column. We have 'payment_method_type'. First lets index it.
		StringIndexer payMethodIndexer = new StringIndexer();
		csvData = payMethodIndexer.setInputCol("payment_method_type")
								  .setOutputCol("payIndex")
								  .fit(csvData)
								  .transform(csvData);
//		Now let's do the same process for 'country'
		StringIndexer countryIndexer = new StringIndexer();
		csvData = countryIndexer.setInputCol("country")
				  				.setOutputCol("countryIndex")
				  				.fit(csvData)
				  				.transform(csvData);
		
		StringIndexer periodIndexer = new StringIndexer();
		csvData = periodIndexer.setInputCol("rebill_period_in_months")
				  				.setOutputCol("periodIndex")
				  				.fit(csvData)
				  				.transform(csvData);
		
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
		csvData = encoder.setInputCols(new String[] {"payIndex", "countryIndex", "periodIndex"})
						 .setOutputCols(new String [] {"payVector", "countryVector", "periodVector"})
						 .fit(csvData)
						 .transform(csvData);
//		csvData.show();
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		Dataset<Row> inputData = vectorAssembler.setInputCols(new String[] {"firstSub", "age", "all_time_views", "last_month_views", 
												   "payVector", "countryVector", "periodVector"})
					   .setOutputCol("features")
					   .transform(csvData)
					   .select("label", "features");
		inputData.show();
		
//		Data is in the right format, so let's start building the model.
		Dataset<Row>[] trainAndHoldoutData = inputData.randomSplit(new double [] {0.9, 0.1});
		Dataset<Row> trainAndTestData = trainAndHoldoutData[0];
		Dataset<Row> holdOutData = trainAndHoldoutData[1];
		
		LinearRegression lr = new LinearRegression();
		ParamGridBuilder pgb = new ParamGridBuilder();
		ParamMap[] paramMap = pgb.addGrid(lr.regParam(), new double[] {0.01, 0.1, 0.3, 0.5, 0.7, 1})
								 .addGrid(lr.elasticNetParam(), new double[] {0, 0.5, 1})
								 .build();
		TrainValidationSplit tvs = new TrainValidationSplit();
		tvs.setEstimator(lr)
		   .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
		   .setEstimatorParamMaps(paramMap)
		   .setTrainRatio(0.9);
		
		TrainValidationSplitModel model = tvs.fit(trainAndTestData);
		LinearRegressionModel lrModel = (LinearRegressionModel) model.bestModel();
		System.out.println("The R2 value is " + lrModel.summary().r2());
		System.out.println("Coefficients = " + lrModel.coefficients() + " and the intercept is = " + lrModel.intercept());
		System.out.println("reg param = " + lrModel.getRegParam() + " and the elastic net param is = " + lrModel.getElasticNetParam());
		
		spark.close();
	}//end main

}//end class
