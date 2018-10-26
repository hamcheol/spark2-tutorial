package com.example.spark.rdd.book.ch6;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class SocketSample {

	public static void main(String[] args) throws InterruptedException {
		JavaSparkContext sc = SparkUtils.getSparkContext("SocketSample");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));
		
		JavaReceiverInputDStream<String> ds = ssc.socketTextStream("localhost", 9000);
		
		ds.print();
		
		ssc.start();
		ssc.awaitTermination();

	}

}
