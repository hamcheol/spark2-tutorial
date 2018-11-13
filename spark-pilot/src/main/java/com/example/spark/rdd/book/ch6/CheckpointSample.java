package com.example.spark.rdd.book.ch6;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class CheckpointSample {

	public static void main(String[] args) throws InterruptedException {
		JavaSparkContext sc = SparkUtils.getSparkContext("WindowSample");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(2));

		ssc.checkpoint("/Users/naver/data/public/temp");
		
		ssc.start();
		ssc.awaitTermination();

	}

}
