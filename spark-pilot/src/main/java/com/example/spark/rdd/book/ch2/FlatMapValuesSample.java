package com.example.spark.rdd.book.ch2;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

import scala.Tuple2;

public class FlatMapValuesSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("FlatMapValuesSample");
		JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer, String>(1, "a,b"), 
			new Tuple2<Integer, String>(2, "a,c"), 
			new Tuple2<Integer, String>(1, "d,e")));
		/*
		JavaPairRDD<Integer, String> pairRDD = rdd1.flatMapValues(t -> {
			return Arrays.asList(t.split(","));
		});
		*/
		JavaPairRDD<Integer, String> pairRDD = rdd1.flatMapValues(t -> Arrays.asList(t.split(",")));
		System.out.println(pairRDD.collect());
	}

}
