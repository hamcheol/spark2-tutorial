package com.example.spark.rdd.book.ch1;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class WorkdCount {

	public static void main(String[] args) {
		if(ArrayUtils.getLength(args) !=3) {
			log.info("Usage: WordCount <Master> <Input> <Output>");
			return;
		}
		
		JavaSparkContext sc = getSparkContext("WordCount", args[0]);
		JavaRDD<String> inputRDD = getInputRDD(sc, args[1]);
		JavaPairRDD<String, Integer> result = process(inputRDD);
		
		handleResult(result, args[2]);

	}

	static void handleResult(JavaPairRDD<String, Integer> result, String output) {
		result.saveAsTextFile(output);
	}

	static JavaPairRDD<String, Integer> process(JavaRDD<String> inputRDD) {
		JavaRDD<String> words = inputRDD.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
		JavaPairRDD<String, Integer> pairRDD = words.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
		JavaPairRDD<String, Integer> resultRDD = pairRDD.reduceByKey((Integer c1, Integer c2) -> c1 + c2);
		
		return resultRDD;
	}

	public static JavaSparkContext getSparkContext(String appName, String master) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> getInputRDD(JavaSparkContext sc, String input) {
		return sc.textFile(input);
	}

}
