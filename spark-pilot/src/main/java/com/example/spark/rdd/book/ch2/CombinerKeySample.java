package com.example.spark.rdd.book.ch2;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.google.common.collect.Lists;

import scala.Tuple2;

public class CombinerKeySample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("CombinerKeySample");
		@SuppressWarnings("unchecked") 
		List<Tuple2<String, Long>> data = Lists.newArrayList(
				new Tuple2<String, Long>("Math",100L),
				new Tuple2<String, Long>("Eng",80L),
				new Tuple2<String, Long>("Math",65L),
				new Tuple2<String, Long>("Math",80L),
				new Tuple2<String, Long>("Eng",55L),
				new Tuple2<String, Long>("Eng",95L)
			);
		
		JavaPairRDD<String, Long> rdd = sc.parallelizePairs(data);
		JavaPairRDD<String, Tuple2<Long, Integer>> rdd2 = rdd.combineByKey((Long v) -> new Tuple2<Long, Integer>(v, 1), 
			(t, v) -> new Tuple2<Long, Integer>(t._1 + v, t._2 + 1), 
			(t1, t2) -> new Tuple2<Long, Integer>(t1._1 + t2._1, t1._2 + t2._2));
		
		System.out.println(rdd2.collect());
		
		JavaPairRDD<String, Double> rdd3 = rdd2.mapValues(tuple -> tuple._1 / (double)tuple._2);
		
		System.out.println(rdd3.collect());

	}

}
