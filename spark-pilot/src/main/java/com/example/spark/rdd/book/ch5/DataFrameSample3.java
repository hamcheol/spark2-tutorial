package com.example.spark.rdd.book.ch5;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.google.common.collect.Lists;

import scala.Tuple2;

public class DataFrameSample3 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("DataSample3");
		String sentence = "Apache Spark is very simple, smart and fast. Have you ever used this application. If you did not, Plz try once.";
		
		Dataset<String> ds1 = session.createDataset(Arrays.asList(sentence), Encoders.STRING());
		Dataset<String> ds1_1 = ds1.flatMap(v -> Arrays.asList(v.split(" ")).iterator(), Encoders.STRING());
		Dataset<Tuple2<String, Object>> result1 = ds1_1.groupByKey(v -> v, Encoders.STRING()).count();
		result1.show();
		
		List<String> words = Lists.newArrayList(sentence.split(" "));
		Dataset<String> ds2 = session.createDataset(words, Encoders.STRING());
		Dataset<Tuple2<String, Object>> result2 = ds1_1.groupByKey(v -> v, Encoders.STRING()).count();
		result2.show();

	}

}
