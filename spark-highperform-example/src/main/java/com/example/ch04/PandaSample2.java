package com.example.ch04;

import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.example.model.PandaPlace;
import com.example.model.RawPanda;
import com.example.utils.SparkUtils;
import com.example.utils.TestPandaProvider;

import jersey.repackaged.com.google.common.collect.Lists;
import scala.Tuple2;

public class PandaSample2 {

	public static void main(String[] args) {
		//SparkSession session = SparkUtils.getSparkSession("PandaSample2");
		JavaSparkContext sc = SparkUtils.getSparkContext("PandaSample2");
		List<PandaPlace> places = TestPandaProvider.extract();
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

		JavaPairRDD<Long, Integer> pair1 = sc.parallelizePairs(tuples1).partitionBy(new Partitioner() {

			private static final long serialVersionUID = -4922357505229677048L;

			@Override
			public int numPartitions() {
				return 4;
			}

			@Override
			public int getPartition(Object key) {
				return (int)(((Long)key) % 4);
			}
		});
		
		JavaPairRDD<Long, String> pair2 = sc.parallelizePairs(tuples2).partitionBy(new Partitioner() {

			private static final long serialVersionUID = -4922357505229677048L;

			@Override
			public int numPartitions() {
				return 4;
			}

			@Override
			public int getPartition(Object key) {
				return (int)(((Long)key) % 4);
			}
		});
		
		JavaPairRDD<Long, Tuple2<Integer, String>> joinPair = pair1.join(pair2);
		
		for(Tuple2<Long, Tuple2<Integer, String>> tuple3 : joinPair.take(100)) {
			System.out.println(tuple3._1 + " " + tuple3._2._1 + " " + tuple3._2._2);
		}


	}
}
