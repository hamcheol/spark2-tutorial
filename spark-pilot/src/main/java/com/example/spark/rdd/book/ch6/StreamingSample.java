package com.example.spark.rdd.book.ch6;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class StreamingSample {

	public static void main(String[] args) throws InterruptedException {
		JavaSparkContext jsc  = SparkUtils.getSparkContext("StreamingSample");
		JavaStreamingContext jstc = new JavaStreamingContext(jsc, Durations.seconds(5));
		
		JavaRDD<String> rdd1 = jsc.parallelize(Arrays.asList("Spark Streaming Sample ssc"));
		JavaRDD<String> rdd2 = jsc.parallelize(Arrays.asList("Spark Queue Spark API"));
		Queue<JavaRDD<String>> queue = new LinkedList<>(Arrays.asList(rdd1, rdd2));
		JavaInputDStream<String> lines = jstc.queueStream(queue, true);
		
		JavaDStream<String> words = lines.flatMap(v -> {
			return Arrays.stream(v.split(" ")).iterator();
		});
		
		words.countByValue().print();
		
		jstc.start();
		jstc.awaitTermination();
		
	}

}
