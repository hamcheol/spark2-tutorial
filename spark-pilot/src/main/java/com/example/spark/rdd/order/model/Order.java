package com.example.spark.rdd.order.model;

import java.io.Serializable;
import java.util.List;

public class Order implements Serializable {
	private static final long serialVersionUID = 1924232719097170032L;
	private String ordNo;
	private String orderTime;
	private Long mbrNo;
	private Integer age;
	private Sex sex;
	private PayMethod payMethod;
	private List<OrderItem> orderItems;

	public String getOrdNo() {
		return ordNo;
	}

	public void setOrdNo(String ordNo) {
		this.ordNo = ordNo;
	}

	public String getOrderTime() {
		return orderTime;
	}

	public void setOrderTime(String orderTime) {
		this.orderTime = orderTime;
	}

	public Long getMbrNo() {
		return mbrNo;
	}

	public void setMbrNo(Long mbrNo) {
		this.mbrNo = mbrNo;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public Sex getSex() {
		return sex;
	}

	public void setSex(Sex sex) {
		this.sex = sex;
	}

	public PayMethod getPayMethod() {
		return payMethod;
	}

	public void setPayMethod(PayMethod payMethod) {
		this.payMethod = payMethod;
	}

	public List<OrderItem> getOrderItems() {
		return orderItems;
	}

	public void setOrderItems(List<OrderItem> orderItems) {
		this.orderItems = orderItems;
	}

}
