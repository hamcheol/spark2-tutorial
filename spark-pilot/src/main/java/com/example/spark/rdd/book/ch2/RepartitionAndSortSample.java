package com.example.spark.rdd.book.ch2;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

import scala.Tuple2;

public class RepartitionAndSortSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("RepartitionAndSortSample", "local[*]");

		JavaPairRDD<Integer, String> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
			.mapToPair((Integer v) -> new Tuple2<Integer, String>(v, "-"));
		
		JavaPairRDD<Integer, String> rdd2 = rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(3));
		
		rdd2.count();
		
		rdd2.foreachPartition((Iterator<Tuple2<Integer, String>> tuples) -> {
			System.out.println("============");
			while(tuples.hasNext()) {
				System.out.println(tuples.next());
			}
		});

	}

}
