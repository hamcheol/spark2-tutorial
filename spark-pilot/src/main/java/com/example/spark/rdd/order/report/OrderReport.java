package com.example.spark.rdd.order.report;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.example.spark.rdd.book.utils.SparkUtils;

public class OrderReport {
	public static void main(String[] args) {
		JavaStreamingContext sc = new JavaStreamingContext(SparkUtils.getSparkContext("OrderReport"), Durations.seconds(3));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
			sc,
			LocationStrategies.PreferConsistent(),
			ConsumerStrategies.<String, String>Subscribe(Arrays.asList("order-queue"),
				SparkUtils.getKafkaParams()));

		JavaDStream<String> kafka = stream.map(ConsumerRecord::value);

		kafka.foreachRDD(v -> {
			v.collect().iterator().forEachRemaining(t -> System.out.println(t));
		});
		/*
		JavaPairDStream<String, String> pair = stream.mapToPair((ConsumerRecord<String, String> v) -> new Tuple2<String, String>(v.key(), v.value()));
		pair.foreachRDD(pv -> {
			pv.groupBy(f);
		});
		*/
		
		sc.start();
		try {
			sc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
