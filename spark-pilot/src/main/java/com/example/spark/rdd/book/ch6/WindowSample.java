package com.example.spark.rdd.book.ch6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class WindowSample {

	public static void main(String[] args) throws InterruptedException {
		JavaSparkContext sc = SparkUtils.getSparkContext("WindowSample");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

		ssc.checkpoint("/Users/naver/data/public/temp");

		List<JavaRDD<Integer>> rdds = new ArrayList<>();
		for (int i = 0; i <= 100; i++) {
			rdds.add(sc.parallelize(Arrays.asList(i)));
		}

		JavaDStream<Integer> ds1 = ssc.queueStream(new LinkedList<>(rdds));
		//6.4.4
		ds1.window(Durations.seconds(6), Durations.seconds(3)).print();
		//6.4.5
		//ds1.countByWindow(Durations.seconds(6), Durations.seconds(3)).print();
		//6.4.6
		ds1.reduceByWindow((v1, v2) -> {
				return v1 + v2;
			},
			Durations.seconds(6),
			Durations.seconds(3)
		).print();

		ssc.start();
		ssc.awaitTermination();

	}

}
