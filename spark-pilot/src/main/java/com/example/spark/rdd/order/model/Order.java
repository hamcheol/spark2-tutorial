package com.example.spark.rdd.order.model;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import lombok.Builder;
import lombok.Data;

@JsonDeserialize(builder=Order.OrderBuilder.class)
@Data
@Builder
public class Order {
	private String ordNo;
	private String orderTime;
	private Long mbrNo;
	private Integer age;
	private Sex sex;
	private PayMethod payMethod;
	private List<OrderItem> orderItems;
	
	@JsonPOJOBuilder(withPrefix = "")
	public static class OrderBuilder {
		
	}

}
