package com.example.spark.rdd.book.ch5;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

import static org.apache.spark.sql.functions.*;

public class DataFrameSample2 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("DataFrameTypeSample2");
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		
		Broadcast<List<Integer>> nums = sc.broadcast(Arrays.asList(1,3,5,7,9));
		Dataset<Long> ds = session.range(0, 10);
		ds.where(col("id").isin(nums.value().toArray())).show();
		
		Dataset<Long> ds2 = session.range(0, 5);
		Column col1 = when(ds2.col("id").mod(2).equalTo(0), "even").otherwise("odd");
		ds2.select(ds2.col("id"), col1.as("type")).show();

	}

}
