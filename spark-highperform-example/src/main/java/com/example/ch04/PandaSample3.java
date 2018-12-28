package com.example.ch04;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.example.model.PandaPlace;
import com.example.model.RawPanda;
import com.example.utils.SparkUtils;
import com.example.utils.TestPandaProvider;
import com.google.common.collect.Lists;

import static org.apache.spark.sql.functions.*;
import scala.Tuple2;

public class PandaSample3 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("PandaSample3");
		
		List<PandaPlace> places = TestPandaProvider.extractPandaPlaces();
		//Dataset<PandaPlace> pandaPlace = session.createDataset(places, Encoders.bean(PandaPlace.class));

		List<Tuple2<Long, Integer>> tuples1 = Lists.newArrayList();
		List<Tuple2<Long, String>> tuples2 = Lists.newArrayList();
		for (PandaPlace place : places) {
			place.getPandas();
			for (RawPanda raw : place.getPandas()) {
				tuples1.add(new Tuple2<Long, Integer>(raw.getId(), raw.getPandaSize()));
				tuples2.add(new Tuple2<Long, String>(raw.getId(), raw.getZip()));
			}
		}
		
		Dataset<Tuple2<Long, Integer>> ds1 = session.createDataset(tuples1, Encoders.tuple(Encoders.LONG(), Encoders.INT())).alias("ds1");
		
		Dataset<Tuple2<Long, String>> ds2 = session.createDataset(tuples2, Encoders.tuple(Encoders.LONG(), Encoders.STRING())).alias("ds2");
		
		ds1.printSchema();
		ds1.show(100);
		ds2.show(100);
		ds1.join(ds2, col("ds1._1").equalTo(col("ds2._1"))).show(100);
		
	}

}
