package com.example.spark.rdd.book.ch2;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class FlatMapSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("FlatMapSample", "local[*]");
		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("apple,orange", "grape,mango,apple", "blueberry,tomato,orange"));
		
		JavaRDD<String> rdd2 = rdd1.flatMap(t -> {
			return Arrays.asList(t.split(",")).iterator();
		});
		
		System.out.println(rdd2.collect());
		
		sc.stop();

	}

}
