package com.example.spark.rdd.book.ch5;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

import static org.apache.spark.sql.functions.*;

public class DataFrameSample {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("DataFrameSample");
		List<Person> persons = Arrays.asList(
				new Person("hayoon", 7, "student"),
				new Person("sunwoo",13,"student"),
				new Person("hajoo",5,"kindergartener"),
				new Person("jinwoo",13,"student")
			);
		Dataset<Row> df = session.createDataFrame(persons, Person.class);
		df.groupBy("job").agg(col("job"), sum("age"), avg("age")).show();
		
		System.out.println(df.head());
		System.out.println(df.take(2));
		System.out.println(df.count());
		System.out.println(df.collectAsList());
		df.describe("age").show();

	}
}
