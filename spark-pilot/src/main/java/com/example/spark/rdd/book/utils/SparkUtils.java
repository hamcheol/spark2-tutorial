package com.example.spark.rdd.book.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
	public static JavaSparkContext getSparkContext(String appName, String master) {
		SparkConf conf = getSparkConf()
			.setAppName(appName)
			.setMaster(master);
		return new JavaSparkContext(conf);
	}
	
	public static SparkSession getSparkSession(String appName, String master) {
		return SparkSession.builder()
			.config(getSparkConf())
			.appName(appName)
			.master(master)
			.getOrCreate();
	}
	
	public static SparkConf getSparkConf() {
		return new SparkConf()
			.set("spark.eventLog.enabled", "true")
			.set("spark.eventLog.dir", "file:/Users/naver/data/public/eventlog")
			.set("spark.history.fs.logDirectory", "file:/Users/naver/data/public/eventlog")
			.set("spark.history.provider", "org.apache.spark.deploy.history.FsHistoryProvider")
			.setJars(new String[]{"/Users/naver/git/spark2-tutorial/spark-pilot/target/spark-pilot-0.0.1-SNAPSHOT.jar"});
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
