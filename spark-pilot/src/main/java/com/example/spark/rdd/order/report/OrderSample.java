package com.example.spark.rdd.order.report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.example.spark.rdd.book.utils.SparkUtils;
import com.example.spark.rdd.order.model.Order;
import com.example.spark.rdd.order.model.OrderItem;
import com.example.spark.rdd.order.model.PayMethod;
import com.google.gson.Gson;

public class OrderSample {
	public static Order getMockData() {
		Order o = new Order();
		o.setOrdNo("1");
		o.setPayMethod(PayMethod.CARD);
		
		OrderItem oi1 = new OrderItem();
		oi1.setPrice(1000L);
		oi1.setOrderCount(2);
		
		OrderItem oi2 = new OrderItem();
		oi2.setPrice(2000L);
		oi2.setOrderCount(1);
		
		o.setOrderItems(Arrays.asList(oi1, oi2));
		
		return o;
	}

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("OrderSample");
		
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
		
		String orderJson = new Gson().toJson(getMockData());
		
		System.out.println(orderJson);
		Row row1 = RowFactory.create(orderJson);
		
		System.out.println(row1.schema().prettyJson());
		Dataset<Row> ds = session.createDataFrame(Arrays.asList(row1), orderType);
		
		ds.schema();
		ds.show();
	}

}
