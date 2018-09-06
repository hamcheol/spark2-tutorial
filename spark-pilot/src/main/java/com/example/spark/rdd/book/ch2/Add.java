package com.example.spark.rdd.book.ch2;

import org.apache.spark.api.java.function.Function;

public class Add implements Function<Integer, Integer> {

	private static final long serialVersionUID = -2519241285896942328L;

	@Override
	public Integer call(Integer v1) throws Exception {
		return v1 + 1;
	}
	
}
