package com.example.spark.rdd.book.ch2;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

import scala.Tuple2;

public class MapValuesSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("MapValuesSample", "local[*]");
		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
		JavaPairRDD<String, Integer> pairRDD = rdd1
			.mapToPair(t -> new Tuple2<String, Integer>(t, 1))
			.mapValues(v -> {
				return v + 1;
			});
		
		System.out.println(pairRDD.collect());

	}

}
