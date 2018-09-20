package com.example.spark.rdd.book.nytimes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

public class NYTimesSample {
	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("NYTimesSample");
		Encoder<Result> resultEncoder = Encoders.bean(Result.class);
		Encoder<Book> bookEncoder = Encoders.bean(Book.class);
		Dataset<Result> df1 = session.read().json("/Users/naver/data/public/nytimes/*").as(resultEncoder);
		/*
		Dataset<Book> df2 = df1.map( (MapFunction<Result, Book>) t -> {
			return t.getBooks();
		}, bookEncoder);
		*/
	}
}
