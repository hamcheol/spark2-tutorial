package com.example.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
	//public static String SPARK_MASTER = "spark://AL01221128.local:7077";
	public static String SPARK_MASTER = "local[3]";

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
			.set("spark.sql.session.timeZone", "Asia/Seoul")
			//.set("spark.driver.allowMultipleContexts", "true")
			.setJars(new String[] {"/Users/naver/git/spark2-tutorial/spark-highperform-example/target/spark-highperform-example-0.0.1-SNAPSHOT.jar"});
	}
	
	public static Map<String, String> getSparkParams() {
		Map<String, String> params = new HashMap<>();
		params.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		params.put("spark.eventLog.enabled", "true");
		params.put("spark.eventLog.dir", "file:/Users/naver/data/public/eventlog");
		params.put("spark.history.fs.logDirectory", "file:/Users/naver/data/public/eventlog");
		params.put("spark.history.provider", "org.apache.spark.deploy.history.FsHistoryProvider");
		params.put("spark.sql.shuffle.partitions", "10");
		return params;
	}
	
	public static Map<String, Object> getKafkaParams() {
		Map<String, Object> params = new HashMap<>();
		params.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		params.put(ConsumerConfig.GROUP_ID_CONFIG, "order-queue-consumer1");
		params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		params.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		return params;
	}
	
	public static Map<String, String> getKafkaStringParams() {
		Map<String, String> params = new HashMap<>();
		params.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "StringDeserializer.class");
		params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "StringDeserializer.class");
		params.put(ConsumerConfig.GROUP_ID_CONFIG, "order-queue-consumer1");
		params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		params.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return params;
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
