package com.example.ch06;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.model.Goldilock;
import com.example.utils.SparkUtils;
import com.example.utils.TestPandaProvider;
import com.google.common.collect.Maps;

public class GoldilockSample1 {

	/**
	 *    * dataframe:
	*   (0.0, 4.5, 7.7, 5.0)
	*   (1.0, 5.5, 6.7, 6.0)
	*   (2.0, 5.5, 1.5, 7.0)
	*   (3.0, 5.5, 0.5, 7.0)
	*   (4.0, 5.5, 0.5, 8.0)
	*
	* targetRanks:
	*   1, 3
	*
	* The output will be:
	*   0 -> (0.0, 2.0)
	*   1 -> (4.5, 5.5)
	*   2 -> (7.7, 1.5)
	*   3 -> (5.0, 7.0)
	 * @param args
	 */
	public static void main(String[] args) {
		//JavaSparkContext sc = SparkUtils.getSparkContext("GoldilockSample1");
		SparkSession session = SparkUtils.getSparkSession("GoldilockSample1");
		List<Goldilock> datas = TestPandaProvider.extractGoldilocks();

		Dataset<Row> df = session.createDataFrame(datas, Goldilock.class).drop("name");
		List<Long> targetRanks = Arrays.asList(2L, 4L);
		
		Map<Integer, List<Double>> result = findRankStatistics(df, targetRanks);
		
		for(Entry<Integer, List<Double>> entry:result.entrySet()) {
			System.out.println(entry.getKey() + "|" + entry.getValue());
		}
	}

	public static Map<Integer, List<Double>> findRankStatistics(Dataset<Row> df, List<Long> targetRanks) {
		String[] cols = df.columns();
		JavaRDD<Row> rdd = df.toJavaRDD();
		Map<Integer, List<Double>> result = Maps.newHashMap();
		
		for (int i = 0; i < cols.length; i++) {
			int idx = i;
			JavaPairRDD<Double, Long> rdd1 = rdd.map((Row t) -> {
				return t.getDouble(idx);
			})
			.sortBy(v -> v, true, 4)
			.zipWithIndex();
			
			System.out.println(rdd1.collect());
			
			List<Double> targetResult = rdd1.filter(t -> targetRanks.contains(t._2 + 1))
				.keys()
				.collect();

			result.put(idx + 1, targetResult);
		}
		return result;
	}

}
