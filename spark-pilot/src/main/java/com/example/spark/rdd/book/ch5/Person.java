package com.example.spark.rdd.book.ch5;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Person implements Serializable {
	private static final long serialVersionUID = 6634222672590685833L;
	
	private String name;
	private int age;
	private String job;
	
}
