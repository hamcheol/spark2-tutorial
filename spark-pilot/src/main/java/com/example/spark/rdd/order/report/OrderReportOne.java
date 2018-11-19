package com.example.spark.rdd.order.report;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.example.spark.rdd.order.model.Order;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderReportOne {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("OrderReportOne");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
			ssc,
			LocationStrategies.PreferConsistent(),
			ConsumerStrategies.<String, String>Subscribe(Arrays.asList("order-queue"),
				SparkUtils.getKafkaParams()));
		
		JavaDStream<Order> kafka = stream.map(v -> new Gson().fromJson(v.value(), Order.class));
		
		Map<Long, Long> map = Maps.newConcurrentMap();
		
		kafka.foreachRDD(rdd -> {
			SparkSession session = SparkSession.builder().config(sc.getConf()).getOrCreate();
			Dataset<Order> raw = session.createDataset(rdd.rdd(), Encoders.bean(Order.class));
			Dataset<Row> explode = raw.select(
				col("ordNo"), 
				col("payMethod"), 
				explode(col("orderItems")).as("oi")
				)
				.withColumn("price", col("oi.price"))
				.withColumn("orderCount", col("oi.orderCount"))
				.withColumn("itemNo", col("oi.itemNo"));
			
			explode.createOrReplaceTempView("order");
			
			Dataset<Row> result = session.sql("select itemNo, sum(price * orderCount) as orderAmt from order group by itemNo");
			List<Row> rowResult = result.collectAsList();
			
			for(Row row : rowResult) {
				long itemNo = row.getLong(0);
				long orderAmt = row.getLong(1);
				if(map.containsKey(itemNo)) {
					orderAmt += map.get(itemNo);
				}
				map.put(itemNo, orderAmt);
			}
			
			List<String> lines = Lists.newArrayList();
			for(Entry<Long, Long> entry : map.entrySet()) {
				lines.add(entry.getKey() + "," + entry.getValue());
			}
			Path path = Paths.get("/Users/naver/data/public/order/report/output/file.txt");
			try {
				Files.write(path, lines, Charset.forName("utf-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/*
			ds.createOrReplaceTempView("order");
			Dataset<Row> ds2 = session.sql("select ordNo, payMethod, explode(orderItems) as oi from order");
			Dataset<Row> ds3 = ds2.withColumn("price", col("oi.price")).withColumn("itemNo", col("oi.itemNo"));
			ds3.explain();
			ds3.printSchema();
			ds3.show();
			*/
			//ds.groupBy(col("orderItems.itemNo")).agg(expr, exprs);
		});
		
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
