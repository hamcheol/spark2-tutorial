package com.example.spark.rdd.order.report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.example.spark.rdd.book.utils.SparkUtils;

import static org.apache.spark.sql.functions.*;

public class OrderReport {
	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getSparkContext("OrderReport");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
			ssc,
			LocationStrategies.PreferConsistent(),
			ConsumerStrategies.<String, String>Subscribe(Arrays.asList("order-queue"),
				SparkUtils.getKafkaParams()));

		JavaDStream<String> kafka = stream.map(v -> v.value());
		
		List<StructField> orderFields = new ArrayList<>();
		orderFields.add(DataTypes.createStructField("ordNo", DataTypes.StringType, true));
		orderFields.add(DataTypes.createStructField("orderTime", DataTypes.StringType, true));
		orderFields.add(DataTypes.createStructField("mbrNo", DataTypes.LongType, true));
		orderFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		orderFields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
		orderFields.add(DataTypes.createStructField("payMethod", DataTypes.StringType, true));
		
		List<StructField> orderItemFields = new ArrayList<>();
		orderItemFields.add(DataTypes.createStructField("itemNo", DataTypes.LongType, true));
		orderItemFields.add(DataTypes.createStructField("itemName", DataTypes.StringType, true));
		orderItemFields.add(DataTypes.createStructField("orderCount", DataTypes.IntegerType, true));
		orderItemFields.add(DataTypes.createStructField("price", DataTypes.LongType, true));
		ArrayType orderItemType = DataTypes.createArrayType(DataTypes.createStructType(orderItemFields), true);
		
		orderFields.add(DataTypes.createStructField("orderItems", orderItemType, true));
		StructType orderType = DataTypes.createStructType(orderFields);
		
		System.out.println(orderType.prettyJson());
		
		kafka.foreachRDD(rdd -> {
			SparkSession session = SparkSession.builder().config(sc.getConf()).getOrCreate();
			Dataset<Row> ds = session.read().schema(orderType).json(rdd.rdd());
			ds.printSchema();
			ds.createOrReplaceTempView("order");
			Dataset<Row> ds2 = session.sql("select ordNo, explode(orderItems) from order");
			ds2.show();
			//ds.show();
			//Dataset<Row> ds2 = session.sql("select payMethod, count(1) from order group by payMethod");
			//Dataset<Row> ds2 = session.sql("select * from order");
			//ds2.printSchema();
			//ds2.show();
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
