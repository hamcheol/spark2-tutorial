package com.example.spark.rdd.book.ch5;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.javalang.typed;

import com.example.spark.rdd.book.utils.SparkUtils;

public class DataFrameSample4 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("DataSample4");
		List<Person> persons = Arrays.asList(
			new Person("hayoon", 7, "student"),
			new Person("sunwoo", 13, "student"),
			new Person("hajoo", 5, "kindergartener"),
			new Person("jinwoo", 13, "student"));

		Dataset<Person> ds = session.createDataset(persons, Encoders.bean(Person.class));
		KeyValueGroupedDataset<Integer, Person> ds1 = ds.groupByKey(v -> v.getAge(), Encoders.INT());
		ds1.count().show();

		KeyValueGroupedDataset<String, Person> ds2 = ds.groupByKey(v -> v.getJob(), Encoders.STRING());
		ds2.agg(typed.sumLong(v -> new Long(v.getAge())), typed.avg(v -> new Double(v.getAge()))).show();

		//ds.createOrReplaceTempView("person");
		//session.sql("select job, sum(age), avg(age) from person group by job").show();
		/*
		KeyValueGroupedDataset<String, String> ds3 = ds2.mapValues(new MapFunction<Person, String>() {
			@Override
			public String call(Person value) throws Exception {
				return p.getName() + "(" + p.getAge() + ")";
			}
			
		}, Encoders.STRING());
		*/
		KeyValueGroupedDataset<String, String> ds3 = ds2.mapValues(p -> {
			return p.getName() + "(" + p.getAge() + ")";
		}, Encoders.STRING());
		
		ds3.reduceGroups((v1, v2) -> {
			return v1 + v2;
		}).show(false);
		
		//return p.getName() + "(" + p.getAge() + ")";

	}

}
