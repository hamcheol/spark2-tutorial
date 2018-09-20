package com.example.spark.rdd.book.ch2;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

import scala.Tuple2;

public class GroupByKeySample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("GroupByKeySample");
		List<Tuple2<String, Integer>> data = Arrays.asList(
			new Tuple2<String, Integer>("a",1),
			new Tuple2<String, Integer>("b",1),
			new Tuple2<String, Integer>("c",1),
			new Tuple2<String, Integer>("b",1),
			new Tuple2<String, Integer>("c",1)
			);
		
		JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data);
		JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupByKey();
		
		System.out.println(rdd2.collect());

	}

}
