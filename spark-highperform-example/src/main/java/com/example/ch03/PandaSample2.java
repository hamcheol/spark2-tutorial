package com.example.ch03;

import org.apache.spark.sql.SparkSession;

import com.example.utils.SparkUtils;

public class PandaSample2 {

	public static void main(String[] args) {
		SparkSession session = SparkUtils.getSparkSession("PandaSample2");

	}

}
