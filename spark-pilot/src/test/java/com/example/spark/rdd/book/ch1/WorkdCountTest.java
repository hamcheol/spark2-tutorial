package com.example.spark.rdd.book.ch1;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

public class WorkdCountTest {
	private static SparkConf conf;
	private static JavaSparkContext sc;
	
	@Before
	public void setUp() {
		conf = new SparkConf().setAppName("WordCountTest").setMaster("local");
		sc = new JavaSparkContext(conf);
	}
	
	@Test
	public void testProcess() {
		List<String> input = Arrays.asList("Apache Spark is fast and general engine for large-scale data processing", 
			"Spark runs on both Windows and UNIX-like systems");
		
		JavaRDD<String> inputRDD = sc.parallelize(input);
		JavaPairRDD<String, Integer> resultRDD = WorkdCount.process(inputRDD);
		Map<String, Integer> resultMap = resultRDD.collectAsMap();
		
		System.out.println(resultMap);
	}
	
	@Test
	public void testMap() {
		String input = "Apache Spark is fast and general engine for large-scale data processing Spark runs on both Windows and UNIX-like systems";
		
		JavaRDD<String> inputRDD = sc.
	}

}
