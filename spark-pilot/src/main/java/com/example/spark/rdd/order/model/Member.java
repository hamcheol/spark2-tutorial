package com.example.spark.rdd.order.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Member {
	private Long mbrNo;
	private Integer age;
	private Sex sex;

}
