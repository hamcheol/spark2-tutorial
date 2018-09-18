package com.example.spark.rdd.book.ch5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import com.example.spark.rdd.book.utils.SparkUtils;

public class SparkSessionSample {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("SparkSessionSample", "spark://AL01221128.local:7077");
		Dataset<Row> df = session.read().text("/Users/naver/data/public/wordcount/input");
		Dataset<Row> wordDf = df.select(explode(split(col("value"), " ")).as("word"));
		Dataset<Row> result = wordDf.groupBy("word").count();
		
		result.foreach(t -> {
			System.out.println(t.mkString());
		});
		
		//System.out.println(result.collect());
	}

}
