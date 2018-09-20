package com.example.spark.rdd.book.nytimes;

import lombok.Data;

@Data
public class Book {
	private Integer rank;
	private String publisher;
	private String author;
	private String title;
	private Integer price;
}
