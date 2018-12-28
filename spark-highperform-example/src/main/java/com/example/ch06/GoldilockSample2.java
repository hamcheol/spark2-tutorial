package com.example.ch06;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.model.Goldilock;
import com.example.utils.SparkUtils;
import com.example.utils.TestPandaProvider;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scala.Tuple2;

public class GoldilockSample2 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("GoldilockSample2");
		List<Goldilock> datas = TestPandaProvider.extractGoldilocks();

		Dataset<Row> df = session.createDataFrame(datas, Goldilock.class).drop("name");

		int colLen = df.schema().length();

		/*
		JavaRDD<Tuple2<Integer, Double>> rdd = df.javaRDD().flatMap(t -> {
			List<Tuple2<Integer, Double>> result = Lists.newArrayList();
			for (int i = 0; i < colLen; i++) {
				result.add(new Tuple2<Integer, Double>(i + 1, t.getDouble(i)));
			}
			return result.iterator();
		});
		*/

		JavaPairRDD<Integer, Double> pair = df.javaRDD().flatMapToPair(t -> {
			List<Tuple2<Integer, Double>> result = Lists.newArrayList();
			for (int i = 0; i < colLen; i++) {
				result.add(new Tuple2<Integer, Double>(i + 1, t.getDouble(i)));
			}
			return result.iterator();
		});

		pair.collect().stream().forEach(t -> {
			System.out.println(t._1 + "," + t._2);
		});

		//groupByKey
		/* 
		JavaPairRDD<Integer, List<Double>> groupedPair = pair.groupByKey().mapValues(t -> {
			List<Double> result = Lists.newArrayList(t);
			Collections.sort(result);
			return result;
		});
		
		groupedPair.collect().stream().forEach(t -> {
			System.out.println(t._1 + "," + t._2);
		});
		*/

		//sortByKey, keys : 동작안됨 
		/*
		JavaRDD<Tuple2<Integer, Double>> pairSorted = pair.mapToPair(t->{
			return new Tuple2<Tuple2<Integer, Double>, Integer>(t, 1);
		}).sortByKey(new Comparator<Tuple2<Integer, Double>>() {
			
			@Override
			public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
				// TODO Auto-generated method stub
				return o1._1.compareTo(o2._1);
			}
		}).keys();
		
		pairSorted.collect().stream().forEach(t -> {
			System.out.println(t._1 + "," + t._2);
		});
		*/

		//repartitionAndSortWithinPartitions
		
		pair.repartitionAndSortWithinPartitions(new ColumnIndexPartition(2))
			.collect()
			.stream()
			.forEach(t -> {
				System.out.println(t._1 + "," + t._2);
			});
		

		/*
		JavaPairRDD<Integer, Double> sorted = pair.repartitionAndSortWithinPartitions(new ColumnIndexPartition(4));

		sorted.mapPartitions(t -> {
			List<Double> values = Lists.newArrayList();
			Lists.newArrayList(t).stream().forEach(v -> {
				values.add(v._2);
			});
			return values.iterator();
		}, true);
		*/


	}

	public static Map<Integer, List<Double>> findRankStatistics(JavaPairRDD<Integer, Double> pair, List<Long> targetRanks, int partitionNum) {
		JavaPairRDD<Integer, Double> sorted = pair.repartitionAndSortWithinPartitions(new ColumnIndexPartition(4));
		
		Map<Integer, List<Double>> result = Maps.newHashMap();
		
		/*
		sorted.mapPartitions(t -> {
			
			List<Double> values = Lists.newArrayList();
			Lists.newArrayList(t).stream().forEach(v -> {
				values.add(v._2);
			});
			return values.iterator();
			
			List<Tuple2<Integer, Double>> list = Lists.newArrayList(t);
			
		}, true);
		*/
		
		return null;
	}

	public static class ColumnIndexPartition extends Partitioner {

		private static final long serialVersionUID = 5127118402481148171L;

		private int numPartitions;

		ColumnIndexPartition(int numPartitions) {
			this.numPartitions = numPartitions;
		}

		@Override
		public int getPartition(Object val) {
			Integer key = (Integer)val;
			return Math.abs(key % numPartitions);
		}

		@Override
		public int numPartitions() {
			return numPartitions;
		}

	}

}
