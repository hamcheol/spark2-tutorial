package com.example.spark.rdd.order.model;

import com.google.common.collect.Lists;

public enum PayMethod {
	CARD,
	BANKING,
	PHONE;
	
	public static PayMethod get(int idx) {
		return Lists.newArrayList(values()).get(idx);
	}
}
