package com.example.spark.rdd.book.ch2;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.google.common.collect.Lists;

import scala.Tuple2;

public class ReduceByKeySample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("ReduceByKeySample");
		@SuppressWarnings("unchecked") 
		List<Tuple2<String, Integer>> data = Lists.newArrayList(
				new Tuple2<String, Integer>("a",1),
				new Tuple2<String, Integer>("b",1),
				new Tuple2<String, Integer>("b",1)
			);
		
		JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data);
		JavaPairRDD<String, Integer> rdd2 = rdd1.reduceByKey((v1,v2) -> v1 + v2);
		
		System.out.println(rdd2.collect());

	}

}
