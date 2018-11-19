package com.example.spark.rdd.book.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructUtils {
	public static StructType getOrderStruct() {
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
		
		return orderType;
	}
}
