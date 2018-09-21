package com.example.spark.rdd.book.nytimes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

import static org.apache.spark.sql.functions.*;

public class NYTimesSample {
	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("NYTimesSample");
		//Encoder<Result> resultEncoder = Encoders.bean(Result.class);
		//Encoder<Book> bookEncoder = Encoders.bean(Book.class);
		Dataset<Row> df = session.read().json("/Users/naver/data/public/nytimes/*");
		/*
		Dataset<Book> df2 = df1.map( (MapFunction<Result, Book>) t -> {
			return t.getBooks();
		}, bookEncoder);
		*/
		
		Dataset<Row> dfExploded = df.select(col("bestsellers_date"), explode(col("books")).as("book"));
		
		dfExploded.groupBy("book.author").agg(count("book"), avg("book.rank")).show();
		
		/*
		dfExploded.createOrReplaceTempView("books");
		session.sql("select ");
		*/
	}
}
