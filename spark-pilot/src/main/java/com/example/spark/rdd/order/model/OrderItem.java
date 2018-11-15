package com.example.spark.rdd.order.model;

import java.io.Serializable;

public class OrderItem implements Serializable {
	private static final long serialVersionUID = 4079656124777282080L;
	private Long itemNo;
	private String itemName;
	private Integer orderCount;
	private Long price;

	public Long getItemNo() {
		return itemNo;
	}

	public void setItemNo(Long itemNo) {
		this.itemNo = itemNo;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public Integer getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(Integer orderCount) {
		this.orderCount = orderCount;
	}

	public Long getPrice() {
		return price;
	}

	public void setPrice(Long price) {
		this.price = price;
	}

}