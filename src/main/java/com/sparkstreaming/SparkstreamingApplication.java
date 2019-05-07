package com.sparkstreaming;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


public class SparkstreamingApplication {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkStream")
			    .setMaster("local[*]");
			  JavaSparkContext sc = new JavaSparkContext(sparkConf);
			  JavaStreamingContext ssc = new JavaStreamingContext(sc, new       Duration(5000));
			  
			  Map<String, String> kafkaParams = new HashMap<String, String>();
			  kafkaParams.put("bootstrap.servers", "localhost:9092");
			  
			  Set<String> topicName = Collections.singleton("test");
			JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils
			    .createDirectStream(ssc, String.class, String.class,
			      StringDecoder.class, StringDecoder.class, kafkaParams,
			      topicName);
			kafkaSparkPairInputDStream.foreachRDD(rdd -> {
		            System.out.println("--- New RDD with " + rdd.partitions().size()
		                    + " partitions and " + rdd.count() + " records" );
		            rdd.foreach(record -> System.out.println("tawfik" + record._2));
		        });
			
			
			
			  ssc.start();
			  ssc.awaitTermination();
			 }
			}
	
		
	