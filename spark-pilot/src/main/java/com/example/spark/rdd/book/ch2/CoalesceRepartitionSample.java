package com.example.spark.rdd.book.ch2;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class CoalesceRepartitionSample {
	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("CoalesceRepartitionSample", "local[*]");
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10), 10);
		JavaRDD<Integer> rdd2 = rdd1.coalesce(5);
		JavaRDD<Integer> rdd3 = rdd1.repartition(10);
		
		System.out.println(rdd1.getNumPartitions());
		System.out.println(rdd2.getNumPartitions());
		System.out.println(rdd3.getNumPartitions());
		
	}

}
