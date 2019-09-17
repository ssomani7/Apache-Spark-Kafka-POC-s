package com.virtualpairprogrammers.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class ViewingFiguresDStreamVersion {

	public static void main(String[] args) throws InterruptedException {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Viewing Figures");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Collection<String> topics = Arrays.asList("viewrecords");
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark-group");
		kafkaParams.put("auto.offset.reset", "latest");
		
//		Connecting JavaStreamingContext a.k.a Spark Streaming to Kafka
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(), 
									  ConsumerStrategies.Subscribe(topics, kafkaParams));
//		JavaDStream<String> results = stream.map(item -> item.value());
//		results.print();
//		JavaPairDStream<String, Long> results = stream.mapToPair(item -> new Tuple2<String, Long>(item.value(), 5L))
//													  .reduceByKey((x,y) -> x + y);
		
//		Use the "swap" method in the earlier section of code where we tried switching RDD generics types
//		In the course views section where we tried to switch the key to the value column and value to key
//		"swap" method is an elegant way to do that instead of creating a whole new intermediate RDD for swapping
//		JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<String, Long>(item.value(), 5L))
//				  									  .reduceByKey((x,y) -> x + y)
//				  									  .mapToPair(item -> item.swap());
		
//		Adding a Window to above line of code
//		JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<String, Long>(item.value(), 5L))
//				  .reduceByKeyAndWindow((x,y) -> x + y, Durations.minutes(60))
//				  .mapToPair(item -> item.swap());
		
//		Experimenting with sliding window, 3rd parameter in the 'reduceByKeyAndWindow' method.
		JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<String, Long>(item.value(), 5L))
				  .reduceByKeyAndWindow((x,y) -> x + y, Durations.minutes(60), Durations.minutes(1))
				  .mapToPair(item -> item.swap());
		
//		Now we need to perform a sort by key operation which is not available on the DStream. But since they
//		are nothing but RDD's under the hood, call the 'transformToPair' method on the DStream to get access to the 
//		RDD methods.
		results = results.transformToPair(rdd -> rdd.sortByKey(false));//false - for Desc sort
		
		results.print(50);
		sc.start();
		sc.awaitTermination();
	}//end main

}//end class
