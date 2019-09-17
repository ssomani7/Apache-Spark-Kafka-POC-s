package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class ViewingFiguresStructuredStreamingVersion {

	public static void main(String[] args) throws StreamingQueryException {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkSession session = SparkSession.builder()
										   .master("local[*]")
										   .appName("structured streaming viewing report")
										   .getOrCreate();
//		Do this when you have a small set of event streaming so as to avoid wastage of default "200" partition
//		shuffles. This will make the batch processing faster.
		session.conf().set("sparl.sql.shuffle.partitions", "10");
		
		Dataset<Row> df = session.readStream()
								 .format("kafka")
								 .option("kafka.bootstrap.servers", "localhost:9092")
								 .option("subscribe", "viewrecords")
								 .load();
//		Creating logical table 'viewing_figures' to deal with spark-sql
		df.createOrReplaceTempView("viewing_figures");
		
//		By creating the logical table you can now use sql sqntax on the SparkSession object.
//		The table 'viewing_figures' will consist of data that is being received from Kafka.
//		Kafka will be sending events consisting of keys, value (name of the training course)
//		and a timestamp. All 3 entities will form separate columns in our table.
//		Dataset<Row> results = session.sql("select value from viewing_figures");
		
//		To convert ByteArrays into String use cast operation in the SQL syntax
		Dataset<Row> results = session.sql("select window, cast(value as string) as course_name, sum(5) as seconds_watched from viewing_figures group by window(timestamp, '2 minutes'), course_name");
//		StreamingQuery query = results.writeStream()
//			   .format("console")
//			   .outputMode(OutputMode.Append())
//			   .start();
//		StreamingQuery query = results.writeStream()
//				   .format("console")
//				   .outputMode(OutputMode.Complete())
//				   .start();
		StreamingQuery query = results.writeStream()
				   .format("console")
				   .outputMode(OutputMode.Update())
				   .option("truncate", false)
				   .option("numRows", 50)
				   .start();
		query.awaitTermination();
	}//end main

}//end class
