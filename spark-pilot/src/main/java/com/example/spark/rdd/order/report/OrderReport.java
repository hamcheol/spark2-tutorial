package com.example.spark.rdd.order.report;

import java.util.Arrays;

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
import com.google.gson.Gson;

public class OrderReport {
	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("OrderReport");
		JavaStreamingContext ssc = new JavaStreamingContext(SparkUtils.getSparkContext("OrderReport"), Durations.seconds(3));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
			ssc,
			LocationStrategies.PreferConsistent(),
			ConsumerStrategies.<String, String>Subscribe(Arrays.asList("order-queue"),
				SparkUtils.getKafkaParams()));

		JavaDStream<Order> kafka = stream.map(v -> {
			return new Gson().fromJson(v.value(), Order.class);
		});

		kafka.foreachRDD(v -> {
			SparkSession session = SparkSession.builder().config(sc.getConf()).getOrCreate();
			//session.createDataFrame(v, Order.class);
			Dataset<Order> ds = session.createDataset(v.rdd(), Encoders.bean(Order.class));
			ds.createOrReplaceTempView("order");
			Dataset<Row> ds2 = session.sql("select payMethod, count(1) from order group by payMethod");
			//Dataset<Row> ds2 = session.sql("select orderItems.itemNo, sum(orderItems.price * orderItems.orderCount) from order group by orderItems.itemNo");
			ds2.show();
			//v.collect().iterator().forEachRemaining(t -> System.out.println(t));
		});
		/*
		JavaPairDStream<String, String> pair = stream.mapToPair((ConsumerRecord<String, String> v) -> new Tuple2<String, String>(v.key(), v.value()));
		pair.foreachRDD(pv -> {
			pv.groupBy(f);
		});
		*/
		
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
