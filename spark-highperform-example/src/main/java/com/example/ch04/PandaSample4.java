package com.example.ch04;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.utils.SparkUtils;

public class PandaSample4 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("PandaSample4");
		List<Panda04> list1 = Arrays.asList(
			new Panda04("Happy", 1.0f), 
			new Panda04("Sad", 0.9f), 
			new Panda04("Happy", 1.5f), 
			new Panda04("Coffee", 3.0f));
		
		List<Panda04> list2 = Arrays.asList(
			new Panda04("Happy", "94110"), 
			new Panda04("Happy", "94103"), 
			new Panda04("Coffee", "10504"), 
			new Panda04("Tea", "07012"));
		
		Dataset<Row> ds1 = session.createDataset(list1, Encoders.bean(Panda04.class)).drop("zip");
		Dataset<Row> ds2 = session.createDataset(list2, Encoders.bean(Panda04.class)).drop("size");
		
		ds1.join(ds2, ds1.col("name").equalTo(ds2.col("name")), "inner").show(100);
		ds1.join(ds2, ds1.col("name").equalTo(ds2.col("name")), "left_outer").show(100);
		ds1.join(ds2, ds1.col("name").equalTo(ds2.col("name")), "right_outer").show(100);
		ds1.join(ds2, ds1.col("name").equalTo(ds2.col("name")), "full_outer").show(100);

	}

}
