package com.example.spark.rdd.book.ch6;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.example.spark.rdd.book.utils.SparkUtils;

import scala.Tuple2;

public class StreamingOps {

	public static void main(String[] args) throws InterruptedException {
		JavaSparkContext sc = SparkUtils.getSparkContext("StreamingOps");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "c", "c"));
		JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("1,2,3,4,5"));
		JavaRDD<String> rdd3 = sc.parallelize(Arrays.asList("k1,r1", "k2,r2", "k3,r3"));
		JavaRDD<String> rdd4 = sc.parallelize(Arrays.asList("k1,s1", "k2,s2"));
		JavaRDD<Integer> rdd5 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

		Queue<JavaRDD<String>> q1 = new LinkedList<>(Arrays.asList(rdd1));
		Queue<JavaRDD<String>> q2 = new LinkedList<>(Arrays.asList(rdd2));
		Queue<JavaRDD<String>> q3 = new LinkedList<>(Arrays.asList(rdd3));
		Queue<JavaRDD<String>> q4 = new LinkedList<>(Arrays.asList(rdd4));
		Queue<JavaRDD<Integer>> q5 = new LinkedList<>(Arrays.asList(rdd5));

		JavaDStream<String> ds1 = ssc.queueStream(q1, true);
		JavaDStream<String> ds2 = ssc.queueStream(q2, true);

		JavaPairDStream<String, String> ds3 = ssc.queueStream(q3, true)
			.mapToPair(t -> {
				String[] arr = t.split(",");
				return new Tuple2<String, String>(arr[0], arr[1]);
			});

		JavaPairDStream<String, String> ds4 = ssc.queueStream(q4, true)
			.mapToPair(t -> {
				String[] arr = t.split(",");
				return new Tuple2<String, String>(arr[0], arr[1]);
			});

		//6.3.2 map(func)
		ds1.map(v -> {
			return "(" + v + ",1)";
		}).print();

		//6.3.3 flatmap
		ds2.flatMap(v -> {
			return Arrays.stream(v.split(",")).iterator();
		}).print();

		//6.3.4 count, countbyvalue
		ds1.count().print();
		ds1.countByValue().print();

		//6.3.5 reduce, reducebykey
		ds1.reduce((v1, v2) -> {
			return v1 + "," + v2;
		}).print();

		ds1.mapToPair(v1 -> new Tuple2<>(v1, 1))
			.reduceByKey((v2, v3) -> {
				return v2 + v3;
		}).print();
		
		//6.3.8 join, union
		ds3.join(ds4).print();
		ds3.union(ds4).print();
		
		ssc.start();
		ssc.awaitTermination();

	}

}
