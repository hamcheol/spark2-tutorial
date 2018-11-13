package com.example.spark.rdd.order.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Item {
	private Long itemNo;
	private String itemName;
	private Long price;
}
