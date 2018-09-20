package com.example.spark.rdd.book.ch2;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class GroupBySample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("GroupBySample");
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
		JavaPairRDD<String, Iterable<Integer>> pairRDD = rdd1.groupBy((Integer v) -> {
			return v % 2 == 0 ? "even" : "odd";
		});
		System.out.println(pairRDD.collect());
	}
}
