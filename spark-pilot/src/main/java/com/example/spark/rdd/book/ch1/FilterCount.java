package com.example.spark.rdd.book.ch1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class FilterCount {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("FilterCount", args[0]);
		JavaRDD<String> inputRDD = SparkUtils.getInputRDD(sc, args[1]);
		
		JavaRDD<String> result = inputRDD.filter(t -> t.contains("error"));
		
		SparkUtils.saveResult(result, args[2]);

	}

}
