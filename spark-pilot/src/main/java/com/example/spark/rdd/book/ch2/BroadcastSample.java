package com.example.spark.rdd.book.ch2;

import java.util.Arrays;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.google.common.collect.Sets;

/**
 * Broardcast는 읽기, Accumulator는 쓰기를 위한 전역 변수 
 * @author naver
 *
 */
public class BroadcastSample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("BroadcastSample", "local[*]");
		Broadcast<Set<String>> broadcast = sc.broadcast(Sets.newHashSet("u1","u2")); 
		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("u1","u3","u3","u4","u5","u6"), 3);
		JavaRDD<String> rdd2 = rdd1.filter(t -> {
			if(broadcast.value().contains(t)) {
				return true;
			} else {
				return false;
			}
		});
		
		System.out.println(rdd2.collect());

	}

}
