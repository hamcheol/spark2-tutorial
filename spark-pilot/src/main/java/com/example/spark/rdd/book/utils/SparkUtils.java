package com.example.spark.rdd.book.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtils {
	public static JavaSparkContext getSparkContext(String appName, String master) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> getInputRDD(JavaSparkContext sc, String input) {
		return sc.textFile(input);
	}
	
	public static void saveResult(JavaPairRDD<String, Integer> result, String output) {
		result.saveAsTextFile(output);
	}
	
	public static void saveResult(JavaRDD<String> result, String output) {
		result.saveAsTextFile(output);
	}
}
