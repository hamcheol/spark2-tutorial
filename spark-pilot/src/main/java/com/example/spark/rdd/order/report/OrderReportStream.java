package com.example.spark.rdd.order.report;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.example.spark.rdd.book.utils.StructUtils;
import com.example.spark.rdd.order.model.Order;
import com.google.gson.Gson;

public class OrderReportStream {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("OrderReportStream");
		//load
		Dataset<Order> ds = session.readStream()
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:9092")
	        .option("subscribe", "order-queue")
	        .option("startingOffsets", "latest")
	        .option("maxOffsetsPerTrigger", 50)
			.load()
			.selectExpr("CAST(value AS STRING)")
			.as(Encoders.STRING())
			.map(v -> {
					return new Gson().fromJson(v, Order.class);
				}, Encoders.bean(Order.class)
			);
		
		ds.printSchema();
		Dataset<Row> pds = ds.select(
			col("ordNo"), 
			col("payMethod"), 
			explode(col("orderItems")).as("oi")
		)
		.withColumn("price", col("oi.price"))
		.withColumn("orderCount", col("oi.orderCount"))
		.withColumn("itemNo", col("oi.itemNo"))
		.groupBy(col("itemNo"))
		.agg(sum(col("price")), sum(col("orderCount")));
		pds.printSchema();
		/*
		Dataset<Row> dsOrder = ds.select(from_json(col("value"), StructUtils.getOrderStruct()))
			.as("order")
			.select(col("order.*"));
		
		dsOrder.printSchema();
		*/
		/*
		Dataset<Row> explode = 	dsOrder.select(
				col("ordNo"), 
				col("payMethod"), 
				explode(col("orderItems")).as("oi")
			)
			.withColumn("price", col("oi.price"))
			.withColumn("orderCount", col("oi.orderCount"))
			.withColumn("itemNo", col("oi.itemNo"));
		
		explode.printSchema();
		*/
		StreamingQuery query = pds.writeStream()
			.format("console")
			.start();
		
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Dataset<Row> result = session.sql("select itemNo, sum(price * orderCount) as orderAmt from order group by itemNo");

	}

}
