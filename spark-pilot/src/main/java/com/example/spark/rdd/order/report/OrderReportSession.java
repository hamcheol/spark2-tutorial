package com.example.spark.rdd.order.report;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

public class OrderReportSession {

	public static void main(String[] args) {
		SparkSession spark = SparkUtils.getSparkSession("OrderReportSession");
		Dataset<Row> ds = spark.readStream()
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:9092")
	        .option("subscribe", "order-queue")
	        .option("startingOffsets", "earliest")
			.load();

	}

}
