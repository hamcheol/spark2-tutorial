package com.example.spark.rdd.book.ch5;

import java.util.Arrays;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.google.common.collect.Lists;

public class DataFrameTypeSample {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("DataFrameTypeSample");
		
		StructField sf1 = DataTypes.createStructField("name", DataTypes.StringType, true);
		StructField sf2 = DataTypes.createStructField("age", DataTypes.IntegerType, true);
		StructField sf3 = DataTypes.createStructField("job", DataTypes.StringType, true);
		StructType schema = DataTypes.createStructType(Lists.newArrayList(sf1, sf2, sf3));

		Row r1 = RowFactory.create("hayoon", 7, "student");
		Row r2 = RowFactory.create("sunwoo", 13, "student");
		Row r3 = RowFactory.create("hajoo", 5, "kindergartener");
		Row r4 = RowFactory.create("jinwoo", 13, "student");

		Dataset<Row> df1 = session.createDataFrame(Arrays.asList(r1,r2,r3,r4), schema);
		
		df1.createOrReplaceTempView("person");
		
		df1.filter((FilterFunction<Row>) t -> t.getInt(1) > 10).show();
		
		session.sql("select * from person where age > 10").show();
	}

}
