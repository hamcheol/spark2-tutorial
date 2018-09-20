package com.example.spark.rdd.book.ch5;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.spark.rdd.book.utils.SparkUtils;

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
		df.groupBy("job").sum("age").show();

	}

	public static class Person {
		private String name;
		private int age;
		private String job;
		
		Person(String name, int age, String job) {
			this.name = name;
			this.age = age;
			this.job = job;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getJob() {
			return job;
		}

		public void setJob(String job) {
			this.job = job;
		}

	}

}
