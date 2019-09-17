package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;//choose this version for Logger
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple1;
import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
//		List<Integer> inputData = new ArrayList<>();
//		inputData.add(35);
//		inputData.add(12);
//		inputData.add(90);
//		inputData.add(20);
		
//		List<String> inputData = new ArrayList<>();
//		inputData.add("WARN: Tuesday 4 September 0405");
//		inputData.add("WARN: Tuesday 4 September 0406");
//		inputData.add("ERROR: Tuesday 4 September 0408");
//		inputData.add("FATAL: Wednesday 5 September 1632");
//		inputData.add("ERROR: Friday 7 September 1854");
//		inputData.add("WARN: Saturday 8 September 1942");
		
		//To handle the session warnings
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		//"local[*] means you are specifying to your machine to use the max available resources/threads for this"
//		SparkConf conf = new SparkConf().setAppName("startingSpark");//For deploying on AWS EMR
		//JavaSparkContext represents the connecetion to the spark cluster
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Parallelize means load the data from collection and turn it into RDD.
//		JavaRDD<Integer> myRDD = sc.parallelize(inputData);
		
		//Since we have declared "myRDD" as double, in Java8 lambda functions, it is assumed that
		//the values passed into the function's parameter are also of the same type as of that the RDD
		//object. So, by default "value1 and value2" are of type Double.
//		Integer result = myRDD.reduce((value1, value2) -> value1 + value2);
		//The "->" arrow sign indicates the implementation of the parameters on the left side
		
//		JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));		
//		sqrtRDD.foreach(value -> System.out.println(value));
//		New Syntax for the above line of code
//		sqrtRDD.collect().forEach(System.out::println);
//		System.out.println("Result = " + result);
//		How to count the number of elements in a RDD using just Map and Reduce?
//		JavaRDD<Long> singleIntegerRDD = sqrtRDD.map(value -> 1L);//Each element gets marked as 1 in a new RDD
//		Long count = singleIntegerRDD.reduce((value1, value2) -> value1 + value2);
//		System.out.println(count);
		
//		JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

//		JavaRDD<Tuple2<Integer, Double>> sqrtRDD = originalIntegers.map(value -> new Tuple2<> (value, Math.sqrt(value)));
		
//		JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
////		It's a commom practice to transform the raw RDD into the Data Structure you requrie as per needs
//		JavaPairRDD<String, Long> pairRDD = originalLogMessages.mapToPair(rawValue -> {
//			String[] columns = rawValue.split(":");
//			String level = columns[0];//This will act as the key. Ex:"FATAL" from the i/p log msg
////			String date = columns[1];//This will act as the value.			
////			return new Tuple2<>(level, date);
//			return new Tuple2<>(level, 1L);
//		});
//		
//		JavaPairRDD<String, Long> sumsRDD = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
//		sumsRDD.foreach(tuple -> System.out.println(tuple._1 + " has "+ tuple._2 + " instances"));
		
//		Below is a scala way of writing the above lambda exceptions with same functionalities. It will replace
//		code from line 64 to line 75
		/*
		sc.parallelize(inputData)
		  .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
		  .reduceByKey((value1, value2) -> value1 + value2)
		  .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		*/
		
//		JavaRDD<String> sentences = sc.parallelize(inputData);
//		JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//		JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
//		filteredWords.foreach(value -> System.out.println(value));
		
//		Below is a scala way of writing the above lambda exceptions with same functionalities. It will replace
//		code from line 87 to line 90
//		sc.parallelize(inputData)
//		  .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
//		  .filter(word -> word.length() > 1)
//		  .foreach(value -> System.out.println(value));
		
		JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
//		initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
//				  .foreach(value -> System.out.println(value));
//		Need the below line for creating bucket on AWS s3
//		JavaRDD<String> initialRDD = sc.textFile("s3n://ssomani-spark-keyword-ranking/input.txt");
		JavaRDD<String> lettersOnlyRDD = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		JavaRDD<String> removedBlankLines = lettersOnlyRDD.filter(sentence -> sentence.trim().length() > 0);
		JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
		JavaRDD<String> justInterestingWords = justWords.filter(word -> Util.isNotBoring(word));
		JavaPairRDD<String, Long> pairRDD = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
		JavaPairRDD<String, Long> totals = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));
		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
//		System.out.println("There are " + sorted.getNumPartitions() + " partitions");
		List<Tuple2<Long, String>> results = sorted.take(50);
		results.forEach(value -> System.out.println(value));

		sc.close();
	}//end main method

}//end class
