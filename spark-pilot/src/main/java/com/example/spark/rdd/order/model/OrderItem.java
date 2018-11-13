package com.example.spark.rdd.order.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import lombok.Builder;
import lombok.Data;

@JsonDeserialize(builder=OrderItem.OrderItemBuilder.class)
@Data
@Builder
public class OrderItem {
	private Long itemNo;
	private String itemName;
	private Integer orderCount;
	private Long price;
	
	@JsonPOJOBuilder(withPrefix = "")
	public static class OrderItemBuilder {
		
	}
}