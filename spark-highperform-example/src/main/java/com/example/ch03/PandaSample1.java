package com.example.ch03;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.example.utils.SparkUtils;
import com.example.utils.TestPandaProvider;
import com.google.common.collect.Lists;

import static org.apache.spark.sql.functions.*;


public class PandaSample1 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("PandaSample1");
		List<PandaPlace> places = TestPandaProvider.extract();
		
		Dataset<PandaPlace> pandaPlace = session.createDataset(places, Encoders.bean(PandaPlace.class));
		
		Dataset<Row> pandaInfo = pandaPlace.select(
				col("name"), 
				explode(col("pandas")) //default alias 'col'
			)
			.withColumn("id", col("col.id"))
			.withColumn("happy", col("col.happy"))
			.withColumn("pt", col("col.pt"))
			.withColumn("zip", col("col.zip"))
			.withColumn("age", col("col.age"))
			.withColumn("pandaSize", col("col.pandaSize"))
			.withColumn("attributes", col("col.attributes"))
			.drop("col");
		
		pandaInfo.show(100);
		//pandaInfo.groupBy("zip").agg(max("pandaSize"), avg("pandaSize"), stddev("pandaSize")).show(100);
		pandaInfo.groupBy("zip").agg(avg("pandaSize").as("pandaSize"), avg("age").as("age")).orderBy(col("age").desc()).show(100);
			
		pandaPlace.write().mode(SaveMode.Overwrite).json("hdfs://localhost:9000/highperform/panda-input/");
	}

	

}
