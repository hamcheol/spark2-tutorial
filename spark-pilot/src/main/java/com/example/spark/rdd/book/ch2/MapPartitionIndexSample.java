package com.example.spark.rdd.book.ch2;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.google.common.collect.Lists;

public class MapPartitionIndexSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("MapPartitionIndexSample", "spark://AL01221128.local:7077");
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
		JavaRDD<Integer> rdd2 = rdd1.mapPartitionsWithIndex((idx, numbers) -> {
			List<Integer> results = Lists.newArrayList();
			if(idx == 1) {
				numbers.forEachRemaining(t -> {
					results.add(t + 1);
				});
			}
			return results.iterator();
		}, true);
		
		List<Integer> results = rdd2.collect();
		System.out.println(results);
		sc.stop();

	}

}
