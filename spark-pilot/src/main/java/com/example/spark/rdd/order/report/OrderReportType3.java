package com.example.spark.rdd.order.report;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.example.spark.rdd.book.utils.StructUtils;

public class OrderReportType3 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("OrderReportType3");
		//load
		Dataset<Row> ds1 = session.readStream()
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:9092")
	        .option("subscribe", "order-queue")
	        .option("startingOffsets", "latest")
	        .option("maxOffsetsPerTrigger", 50)
			.load()
			.selectExpr("CAST(value as string) as json", "current_timestamp as ts")
			.select(
				from_json(col("json"), StructUtils.getOrderStruct()).as("order"), 
				col("ts")
			)
			.select(col("order.*"), col("ts"));
		
		ds1.printSchema();
		
		Dataset<Row> ds2 = ds1.select(
			col("ordNo"), 
			col("payMethod"),
			col("ts"),
			explode(col("orderItems")).as("oi"),
			window(col("ts"), "3 minute", "1 minute").as("window")
		)
		.withColumn("price", col("oi.price"))
		.withColumn("orderCount", col("oi.orderCount"))
		.withColumn("itemNo", col("oi.itemNo"))
		.withWatermark("ts", "3 minute");
		ds2.printSchema();
		
		Dataset<Row> ds3 = ds2.groupBy(col("window"), col("oi.itemNo"))
			.agg(sum(col("oi.price")), sum(col("oi.orderCount")));
		
		ds3.printSchema();
		
		StreamingQuery query = ds3.writeStream()
			.outputMode(OutputMode.Complete())
			.trigger(Trigger.ProcessingTime("10 seconds"))
			.format("console")
			.start();
		
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
