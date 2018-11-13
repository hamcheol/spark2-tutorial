package com.example.spark.rdd.book.ch6;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.example.spark.rdd.book.utils.SparkUtils;

import scala.Tuple2;

public class StreamingOps2 {

	public static void main(String[] args) throws InterruptedException {
		JavaSparkContext sc = SparkUtils.getSparkContext("StreamingOps2");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

		// 6.4.2절
		JavaRDD<String> t1 = sc.parallelize(Arrays.asList("a", "b", "c"));
		JavaRDD<String> t2 = sc.parallelize(Arrays.asList("b", "c"));
		JavaRDD<String> t3 = sc.parallelize(Arrays.asList("a", "a", "a"));
		Queue<JavaRDD<String>> q6 = new LinkedList<>(Arrays.asList(t1, t2, t3));
		JavaDStream<String> ds6 = ssc.queueStream(q6, true);
		
		ssc.checkpoint("/Users/naver/data/public/temp");

		ds6.mapToPair(v1 -> new Tuple2<>(v1, 1))
			.updateStateByKey((List<Integer> list, Optional<Integer> curVal) -> {
				return Optional.of(curVal.orElse(0) + list.stream().reduce(0, Integer::sum));
			})
			.print();
		
		/*
	    ds6.mapToPair((String s) -> new Tuple2<String, Long>(s, 1L))
	            .updateStateByKey((List<Long> newValues, Optional<Long> currentValue) -> {
	              return Optional.of(currentValue.orElse(0L) + newValues.stream().reduce(0L, Long::sum));
	            }).print();
	    */
		
		ssc.start();
		ssc.awaitTermination();

	}

}
