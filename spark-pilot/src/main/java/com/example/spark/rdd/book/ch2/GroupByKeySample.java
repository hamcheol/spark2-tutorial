package com.example.spark.rdd.book.ch2;

import org.apache.spark.api.java.JavaSparkContext;

import com.example.spark.rdd.book.utils.SparkUtils;

public class GroupByKeySample {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("GroupByKeySample", "local[*]");

	}

}
