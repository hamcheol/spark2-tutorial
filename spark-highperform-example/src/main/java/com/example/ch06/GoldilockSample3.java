package com.example.ch06;

import java.util.List;
import java.util.Map;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.ch06.GoldilockSample2.ColumnIndexPartition;
import com.example.model.Goldilock;
import com.example.utils.SparkUtils;
import com.example.utils.TestPandaProvider;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scala.Tuple2;

public class GoldilockSample3 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("GoldilockSample3");
		List<Goldilock> datas = TestPandaProvider.extractGoldilocks();

		Dataset<Row> df = session.createDataFrame(datas, Goldilock.class).drop("name");

	}
	
	public static Map<Integer, List<Double>> findRankStatistics(Dataset<Row> df, List<Long> targetRanks, int partitionNum) {
		int colLen = df.schema().length();
		
		JavaPairRDD<Integer, Double> pair = df.javaRDD().flatMapToPair(t -> {
			List<Tuple2<Integer, Double>> result = Lists.newArrayList();
			for (int i = 0; i < colLen; i++) {
				result.add(new Tuple2<Integer, Double>(i + 1, t.getDouble(i)));
			}
			return result.iterator();
		});
		
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
