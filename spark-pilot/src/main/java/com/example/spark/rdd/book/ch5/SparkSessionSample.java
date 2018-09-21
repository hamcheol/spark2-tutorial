package com.example.spark.rdd.book.ch5;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

public class SparkSessionSample {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("SparkSessionSample");
		Dataset<Row> df = session.read().text("/Users/naver/data/public/wordcount/input");
		Dataset<Row> wordDf = df.select(explode(split(col("value"), " ")).as("word"));
		Dataset<Row> result = wordDf.groupBy("word").count();
		
		result.foreach(t -> {
			System.out.println(t.mkString());
		});
		
		//System.out.println(result.collect());
	}

}
