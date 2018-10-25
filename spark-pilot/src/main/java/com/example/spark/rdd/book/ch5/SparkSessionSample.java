package com.example.spark.rdd.book.ch5;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

import scala.Tuple2;

public class SparkSessionSample {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("SparkSessionSample");
		Dataset<Row> df = session.read().text("/Users/naver/data/public/wordcount/input");

		runUntypedTransformationsExample(session, df);
		runTypedTransformationsExample(session, df);
	}

	public static void runUntypedTransformationsExample(SparkSession spark, Dataset<Row> df) {
		Dataset<Row> wordDf = df.select(explode(split(col("value"), " ")).as("word"));
		Dataset<Row> result = wordDf.groupBy("word").count();
		
		result.show();

	}

	public static void runTypedTransformationsExample(SparkSession spark, Dataset<Row> df) {
		Dataset<String> ds = df.as(Encoders.STRING());
		Dataset<String> wordDf = ds.flatMap(v -> {
			return Arrays.asList(v.split(" ")).iterator();
		}, Encoders.STRING());
		
		Dataset<Tuple2<String, Object>> result = wordDf.groupByKey((String v) -> v, Encoders.STRING()).count();

		result.show();

	}

}
