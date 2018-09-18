package com.example.spark.rdd.book.nytimes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

public class NYTimesSample {
	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("NYTimesSample", "spark://AL01221128.local:7077");
		Dataset<Row> df = session.read().json("/Users/naver/data/public/nytimes/*");
	}
}
