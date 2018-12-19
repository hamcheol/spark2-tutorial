package com.example.ch03;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.utils.SparkUtils;
import com.google.common.collect.Lists;

import static org.apache.spark.sql.functions.*;


public class PandaSample1 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("PandaSample1");
		List<PandaPlace> places = Lists.newArrayList();
		String[] name = {"Beijing", "Seoul", "Tokyo"};
		String[] zip = {"94110","95420","96018"};
		String[] pt = {"giant","red"};

		for (int i = 0; i < 3; i++) {
			List<RawPanda> raws = Lists.newArrayList();
			for (int j = 0; j < 5; j++) {
				RawPanda raw = new RawPanda();
				raw.setId(new Long((i + 1) * 10 + j));
				raw.setHappy(j % 2 == 0);
				raw.setPt(pt[RandomUtils.nextInt(0, 2)]);
				raw.setZip(zip[RandomUtils.nextInt(0, 3)]);
				raw.setPandaSize(RandomUtils.nextInt(150, 200));
				raw.setAttributes(Lists.newArrayList((i+1)/(double)(i+2), (j+1)/(double)(j+2)));
				raws.add(raw);
			}
			places.add(new PandaPlace(name[RandomUtils.nextInt(0, 3)], raws));
		}
		
		Dataset<PandaPlace> pandaPlace = session.createDataset(places, Encoders.bean(PandaPlace.class));
		
		Dataset<Row> pandaInfo = pandaPlace.select(
				col("name"), 
				explode(col("pandas")) //default alias 'col'
			)
			.withColumn("id", col("col.id"))
			.withColumn("happy", col("col.happy"))
			.withColumn("pt", col("col.pt"))
			.withColumn("zip", col("col.zip"))
			.withColumn("pandaSize", col("col.pandaSize"))
			.withColumn("attributes", col("col.attributes"))
			.drop("col");
		
		pandaInfo.printSchema();
		pandaInfo.show();
		
		pandaInfo.filter(col("happy").equalTo(true)).show();
	}

}
