package com.example.spark.rdd.book.ch2;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class PassingFunctionSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("PassingFunctionSample", "local[*]");
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
		JavaRDD<Integer> rdd2 = rdd1.map(new Add());

		StringBuilder builder = new StringBuilder();
		rdd2.collect().stream().forEach(t -> {
			builder.append(t).append(" ");
		});
		System.out.println("result : " + builder.toString());
		sc.stop();
		sc.close();
	}
}
