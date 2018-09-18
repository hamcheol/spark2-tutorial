package com.example.spark.rdd.book.ch2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.google.common.collect.Lists;

public class MapPartitionSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("MapPartitionSample", "spark://AL01221128.local:7077");
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

		JavaRDD<Integer> rdd2 = rdd1.mapPartitions((Iterator<Integer> t) -> {
			System.out.println("map partition function executed.");
			List<Integer> results = Lists.newArrayList();
			t.forEachRemaining(i -> results.add(i + 1));
			return results.iterator();
		});
		
		//List<Integer> results = rdd2.collect();
		
		System.out.println("results : " + rdd2.collect());

	}

}
