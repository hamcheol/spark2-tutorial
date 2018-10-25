package com.example.spark.rdd.book.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
	public static String SPARK_MASTER = "spark://AL01221128.local:7077";

	public static JavaSparkContext getSparkContext(String appName) {
		SparkConf conf = getSparkConf()
			.setAppName(appName)
			.setMaster(SPARK_MASTER);
		return new JavaSparkContext(conf);
	}

	public static SparkSession getSparkSession(String appName) {
		return SparkSession.builder()
			.config(getSparkConf())
			.appName(appName)
			.master(SPARK_MASTER)
			.getOrCreate();
	}

	public static SparkConf getSparkConf() {
		return new SparkConf()
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.eventLog.enabled", "true")
			.set("spark.eventLog.dir", "file:/Users/naver/data/public/eventlog")
			.set("spark.history.fs.logDirectory", "file:/Users/naver/data/public/eventlog")
			.set("spark.history.provider", "org.apache.spark.deploy.history.FsHistoryProvider")
			.set("spark.sql.shuffle.partitions", "10")
			.setJars(new String[] {"/Users/naver/git/spark2-tutorial/spark-pilot/target/spark-pilot-0.0.1-SNAPSHOT.jar"});
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
