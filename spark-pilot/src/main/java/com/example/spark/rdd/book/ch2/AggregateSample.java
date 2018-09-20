package com.example.spark.rdd.book.ch2;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

import scala.Tuple2;

public class AggregateSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("AggregateSample");
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(40, 50, 30, 80, 90, 80, 60), 3);
		Tuple2<Integer, Integer> result = rdd1.aggregate(new Tuple2<Integer, Integer>(0, 0),
			(tu, v) -> new Tuple2<Integer, Integer>(tu._1 + v, tu._2 + 1),
			(tu1, tu2) -> new Tuple2<>(tu1._1 + tu2._1, tu1._2 + tu2._2));
		
		double avg = (result._1 / (double)result._2);
		System.out.println(avg);

	}

}
