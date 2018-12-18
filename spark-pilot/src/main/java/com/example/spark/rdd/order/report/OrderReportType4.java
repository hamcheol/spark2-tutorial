package com.example.spark.rdd.order.report;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.window;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.example.spark.rdd.book.utils.StructUtils;

public class OrderReportType4 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("OrderReportType4");
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
		
		Dataset<Row> ds2 = ds1.select(
				col("ordNo"),
				col("mbrNo"),
				col("payMethod"),
				col("orderTime"),
				col("ts"),
				explode(col("orderItems")).as("oi")
			)
			.withColumn("price", col("oi.price"))
			.withColumn("orderCount", col("oi.orderCount"))
			.withColumn("itemNo", col("oi.itemNo"))
			.withColumn("ots", unix_timestamp(col("orderTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS").cast(DataTypes.TimestampType));
		
		Dataset<Row> orderDs = ds2.withWatermark("ots", "3 minutes")
			.groupBy(window(col("ots"), "3 minute", "1 minute"), col("oi.itemNo"))
			.agg(sum(col("oi.price")), sum(col("oi.orderCount")));
		
		StreamingQuery query1 = orderDs.writeStream()
			.outputMode(OutputMode.Update())
			.trigger(Trigger.ProcessingTime("10 seconds"))
			.format("console")
			.option("truncate", "false")
			.option("numRows", "100")
			.option("checkpointLocation", "/Users/naver/data/public/temp2")
			.start();
		
		Dataset<Row> memberDs = ds2.withWatermark("ots", "3 minutes")
			.groupBy(window(col("ots"), "3 minute", "1 minute"), col("mbrNo"))
			.agg(sum(col("oi.price")), sum(col("oi.orderCount")));
			
		StreamingQuery query2 = memberDs.writeStream()
			.outputMode(OutputMode.Update())
			.trigger(Trigger.ProcessingTime("10 seconds"))
			.format("console")
			.option("truncate", "false")
			.option("numRows", "100")
			.option("checkpointLocation", "/Users/naver/data/public/temp3")
			.start();
		
		StreamingQuery query3 = ds2.writeStream()
			.outputMode(OutputMode.Append())
			.trigger(Trigger.ProcessingTime("10 seconds"))
			.format("json")
			.option("path", "/Users/naver/data/public/order/report/output/raw")
			.option("checkpointLocation", "/Users/naver/data/public/temp4")
			.start();
		
		try {
			query1.awaitTermination();
			query2.awaitTermination();
			query3.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
